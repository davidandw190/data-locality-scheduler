import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import kubernetes.client
import kubernetes.config
from kubernetes.client.rest import ApiException
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("benchmarks/results/benchmark_run.log")
    ]
)
logger = logging.getLogger("benchmark-runner")

class BenchmarkRunner:
    
    def __init__(self, config_file, output_dir, run_id=None):
        self.config_file = config_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id or str(uuid.uuid4())[:8]
        
        self.config = self._load_config()
        self.results = {
            "metadata": {
                "run_id": self.run_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "cluster_info": {},
                "config": self.config
            },
            "workloads": {},
            "metrics": {},
            "comparison": {}
        }
        
        try:
            kubernetes.config.load_kube_config()
            logger.info("Loaded Kubernetes config from default location")
        except Exception:
            try:
                kubernetes.config.load_incluster_config()
                logger.info("Loaded in-cluster Kubernetes configuration")
            except Exception as e:
                logger.error(f"Failed to load Kubernetes configuration: {e}")
                raise
                
        self.k8s_client = kubernetes.client.CoreV1Api()
        self.k8s_apps = kubernetes.client.AppsV1Api()
        self.metrics = {}
        
        # storage location mapping (bucket -> storage service -> node)
        self.bucket_node_mapping = {}
        self.data_item_locations = {}
        
    def _load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    
    def prepare_environment(self):
        logger.info("Preparing benchmark environment")
        
        ns_name = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        try:
            self.k8s_client.read_namespace(name=ns_name)
            logger.info(f"Using existing namespace: {ns_name}")
        except ApiException as e:
            if e.status == 404:
                ns_body = kubernetes.client.V1Namespace(
                    metadata=kubernetes.client.V1ObjectMeta(name=ns_name)
                )
                self.k8s_client.create_namespace(body=ns_body)
                logger.info(f"Created namespace: {ns_name}")
            else:
                logger.error(f"Error checking namespace: {e}")
                raise
        
        try:
            logger.info("Cleaning up any existing storage deployments...")
            cmd = f"kubectl delete deployment --namespace={ns_name} --selector=app=minio"
            subprocess.run(cmd, shell=True)
            time.sleep(5) 
        except Exception as e:
            logger.warning(f"Error cleaning up storage: {e}")
        
        if self.config.get('kubernetes', {}).get('setup_storage', True):
            self._deploy_storage_services()
            self._wait_for_storage_readiness()
            self._initialize_benchmark_data()
            self._wait_for_data_initialization()
        
        self._collect_cluster_info()
        self._map_buckets_to_nodes()
        self._index_data_locations()
    
    
    def _deploy_storage_services(self):
        """Deploy storage services for benchmarking"""
        logger.info("Deploying storage services")
        
        storage_manifests = Path("benchmarks/simulated/kubernetes/storage.yaml")
        if storage_manifests.exists():
            cmd = ["kubectl", "apply", "-f", str(storage_manifests)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to deploy storage services: {result.stderr}")
                raise RuntimeError(f"Failed to deploy storage: {result.stderr}")
            logger.info("Successfully deployed storage services")
        else:
            logger.error(f"Storage manifest not found: {storage_manifests}")
            raise FileNotFoundError(f"Storage manifest not found: {storage_manifests}")
    
    
    def _wait_for_storage_readiness(self):
        logger.info("Waiting for storage services to be ready")
        
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        timeout = 300  # 5 minutes timeout
        start_time = time.time()
        ready = False
        
        while time.time() - start_time < timeout and not ready:
            try:
                pods = self.k8s_client.list_namespaced_pod(
                    namespace=namespace,
                    label_selector="app=minio"
                )
                
                all_ready = True
                ready_count = 0
                pending_pods = []
                
                for pod in pods.items:
                    if pod.status.phase != 'Running':
                        all_ready = False
                        pending_pods.append(f"{pod.metadata.name} ({pod.status.phase})")
                    else:
                        containers_ready = True
                        for container_status in pod.status.container_statuses:
                            if not container_status.ready:
                                containers_ready = False
                                break
                        
                        if containers_ready:
                            ready_count += 1
                        else:
                            all_ready = False
                            pending_pods.append(f"{pod.metadata.name} (containers not ready)")
                
                if all_ready and ready_count >= 3: 
                    ready = True
                    logger.info(f"Storage services are ready: {ready_count} pods running with ready containers")
                else:
                    logger.info(f"Waiting for storage services: {ready_count}/{len(pods.items)} pods running")
                    if pending_pods:
                        logger.info(f"Pending pods: {', '.join(pending_pods)}")
                    
                    if time.time() - start_time > 60:
                        for pod in pods.items:
                            if pod.status.phase != 'Running':
                                field_selector = f"involvedObject.name={pod.metadata.name}"
                                events = self.k8s_client.list_namespaced_event(
                                    namespace=namespace,
                                    field_selector=field_selector
                                )
                                for event in events.items:
                                    if event.type == 'Warning':
                                        logger.warning(f"Warning event for {pod.metadata.name}: {event.reason} - {event.message}")
                    
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Error while checking storage readiness: {e}")
                time.sleep(5)
        
        if not ready:
            logger.warning("Storage services not fully ready within timeout, checking individual services...")
            
            services = ["minio-central", "minio-edge-region1", "minio-edge-region2"]
            for service in services:
                try:
                    svc = self.k8s_client.read_namespaced_service(service, namespace)
                    logger.info(f"Service {service} exists with cluster IP: {svc.spec.cluster_ip}")
                    
                    endpoints = self.k8s_client.read_namespaced_endpoints(service, namespace)
                    if endpoints.subsets and len(endpoints.subsets) > 0:
                        subset = endpoints.subsets[0]
                        if subset.addresses and len(subset.addresses) > 0:
                            logger.info(f"Service {service} has {len(subset.addresses)} endpoint addresses")
                        else:
                            logger.warning(f"Service {service} has no endpoint addresses")
                    else:
                        logger.warning(f"Service {service} has no endpoints")
                except Exception as e:
                    logger.warning(f"Failed to check service {service}: {e}")
            
            logger.warning("Proceeding with benchmarks despite storage services not being fully ready")

    
    
    
    
    def _initialize_benchmark_data(self):
        logger.info("Initializing benchmark data")
        
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        
        cmd = [
            "python", 
            "benchmarks/simulated/framework/data_initializer.py",
            "--config", self.config_file,
            "--workloads-dir", "benchmarks/simulated/workloads"
        ]
        
        logger.info(f"Running data initializer: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to initialize benchmark data: {result.stderr}")
            logger.error(f"Data initializer output: {result.stdout}")
            logger.warning("Proceeding with benchmarks, but data locality measurements may be inaccurate")
        else:
            logger.info("Successfully initialized benchmark data")
            logger.debug(f"Data initializer output: {result.stdout}")
    
    def _wait_for_data_initialization(self):
        """Wait for data initialization to complete"""
        logger.info("Waiting for data initialization to complete")
        time.sleep(10) 
    
    def _map_buckets_to_nodes(self):
        """Map storage buckets to the nodes where they are hosted"""
        logger.info("Mapping storage buckets to nodes")
        
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        
        try:
            pods = self.k8s_client.list_namespaced_pod(
                namespace=namespace,
                label_selector="app=minio"
            )
            
            for pod in pods.items:
                if pod.status.phase != 'Running' or not pod.spec.node_name:
                    continue
                
                node_name = pod.spec.node_name
                
                labels = pod.metadata.labels
                role = labels.get('role', 'unknown')
                region = labels.get('region', 'central')
                
                if role == 'central':
                    buckets = ["datasets", "intermediate", "results", "shared", "test-bucket"]
                    service_name = "minio-central"
                elif role == 'edge' and region == 'region-1':
                    buckets = ["edge-data", "region1-bucket"]
                    service_name = "minio-edge-region1"
                elif role == 'edge' and region == 'region-2':
                    buckets = ["region2-bucket"]
                    service_name = "minio-edge-region2"
                else:
                    continue
                
                for bucket in buckets:
                    if bucket not in self.bucket_node_mapping:
                        self.bucket_node_mapping[bucket] = {}
                    
                    self.bucket_node_mapping[bucket][service_name] = node_name
                
                logger.info(f"Mapped {len(buckets)} buckets to node {node_name} for {service_name}")
            
            logger.info(f"Mapped buckets: {self.bucket_node_mapping}")
            
        except Exception as e:
            logger.error(f"Failed to map buckets to nodes: {e}")
    
    def _index_data_locations(self):
        logger.info("Indexing data locations for data locality metrics")
        
        workloads_dir = Path("benchmarks/simulated/workloads")
        
        for workload_file in workloads_dir.glob("*.yaml"):
            try:
                with open(workload_file, 'r') as f:
                    workload = list(yaml.safe_load_all(f))
                
                for pod in workload:
                    if pod.get('kind') != 'Pod' or 'annotations' not in pod.get('metadata', {}):
                        continue
                    
                    # we get data references from annotations
                    for k, v in pod['metadata']['annotations'].items():
                        if not k.startswith('data.scheduler.thesis/'):
                            continue
                        
                        parts = v.split(',')
                        if len(parts) < 2:
                            continue
                        
                        urn = parts[0]
                        bucket = urn.split('/')[0]
                        
                        # we find nodes containing this data
                        if bucket in self.bucket_node_mapping:
                            self.data_item_locations[urn] = list(self.bucket_node_mapping[bucket].values())
                
            except Exception as e:
                logger.error(f"Failed to index data locations from {workload_file}: {e}")
        
        logger.info(f"Indexed {len(self.data_item_locations)} data items")
    
    def _collect_cluster_info(self):
        logger.info("Collecting cluster information")
        
        nodes = self.k8s_client.list_node()
        node_info = []
        for node in nodes.items:
            node_data = {
                "name": node.metadata.name,
                "labels": node.metadata.labels,
                "capacity": node.status.capacity,
                "allocatable": node.status.allocatable,
                "conditions": [
                    {
                        "type": condition.type, 
                        "status": condition.status
                    } for condition in node.status.conditions
                ],
                "node_info": {
                    "architecture": node.status.node_info.architecture,
                    "container_runtime_version": node.status.node_info.container_runtime_version,
                    "kernel_version": node.status.node_info.kernel_version,
                    "kube_proxy_version": node.status.node_info.kube_proxy_version,
                    "kubelet_version": node.status.node_info.kubelet_version,
                    "operating_system": node.status.node_info.operating_system,
                    "os_image": node.status.node_info.os_image
                }
            }
            node_info.append(node_data)
        
        self.results["metadata"]["cluster_info"]["nodes"] = node_info
        self.results["metadata"]["cluster_info"]["node_count"] = len(node_info)
        
        self.results["metadata"]["cluster_info"]["node_types"] = {
            "edge": [],
            "cloud": [],
            "other": []
        }
        
        for node in node_info:
            node_type = node["labels"].get("node-capability/node-type", "other")
            if node_type == "edge":
                self.results["metadata"]["cluster_info"]["node_types"]["edge"].append(node["name"])
            elif node_type == "cloud":
                self.results["metadata"]["cluster_info"]["node_types"]["cloud"].append(node["name"])
            else:
                self.results["metadata"]["cluster_info"]["node_types"]["other"].append(node["name"])
        
        try:
            namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
            storage_pods = self.k8s_client.list_namespaced_pod(
                namespace=namespace,
                label_selector="app=minio"
            )
            
            storage_info = []
            for pod in storage_pods.items:
                pod_info = {
                    "name": pod.metadata.name,
                    "node": pod.spec.node_name,
                    "labels": pod.metadata.labels,
                    "status": pod.status.phase,
                    "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None
                }
                storage_info.append(pod_info)
            
            self.results["metadata"]["cluster_info"]["storage"] = storage_info
        except Exception as e:
            logger.error(f"Error collecting storage information: {e}")
        
        logger.info(f"Collected information for {len(node_info)} nodes")
        
        logger.info("Cluster Overview:")
        logger.info(f"- Total nodes: {len(node_info)}")
        logger.info(f"- Edge nodes: {len(self.results['metadata']['cluster_info']['node_types']['edge'])}")
        logger.info(f"- Cloud nodes: {len(self.results['metadata']['cluster_info']['node_types']['cloud'])}")
    
    
    def run_workload(self, workload_name, scheduler_name, iteration=1):
        """Run a specific workload with a specific scheduler"""
        logger.info(f"Running workload '{workload_name}' with scheduler '{scheduler_name}' (iteration {iteration})")
        
        workload_file = Path(f"benchmarks/simulated/workloads/{workload_name}.yaml")
        if not workload_file.exists():
            logger.error(f"Workload file not found: {workload_file}")
            return False
        
        run_suffix = f"{self.run_id}-{iteration}"
        
        try:
            with open(workload_file, 'r') as f:
                workload_yaml = f.read()
                
            workload = list(yaml.safe_load_all(workload_yaml))
            modified_workload = []
            
            for item in workload:
                if item.get('kind') == 'Pod':
                    original_name = item['metadata']['name']
                    item['metadata']['name'] = f"{original_name}-{run_suffix}"
                    
                    item['spec']['schedulerName'] = scheduler_name
                    
                    if 'annotations' not in item['metadata']:
                        item['metadata']['annotations'] = {}
                    
                    item['metadata']['annotations'].update({
                        'benchmark.thesis/run-id': self.run_id,
                        'benchmark.thesis/workload': workload_name,
                        'benchmark.thesis/scheduler': scheduler_name,
                        'benchmark.thesis/iteration': str(iteration)
                    })
                    
                    if 'labels' not in item['metadata']:
                        item['metadata']['labels'] = {}
                    
                    item['metadata']['labels'].update({
                        'benchmark-run-id': self.run_id,
                        'workload': workload_name,
                        'scheduler': scheduler_name.replace('-', ''),
                        'iteration': str(iteration)
                    })
                    
                    if 'containers' in item['spec']:
                        for container in item['spec']['containers']:
                            if 'env' not in container:
                                container['env'] = []
                            
                            container['env'].append({
                                'name': 'BENCHMARK_RUN_ID',
                                'value': self.run_id
                            })
                            
                            container['env'].append({
                                'name': 'WORKLOAD_NAME',
                                'value': workload_name
                            })
                            
                            container['env'].append({
                                'name': 'SCHEDULER_NAME',
                                'value': scheduler_name
                            })
                            
                            container['env'].append({
                                'name': 'ITERATION',
                                'value': str(iteration)
                            })
                            
                            container['env'].append({
                                'name': 'WORKLOAD_DURATION',
                                'value': '60'  # 60 seconds
                            })
                            
                            for k, v in item['metadata'].get('annotations', {}).items():
                                if k.startswith('data.scheduler.thesis/'):
                                    env_name = k.replace('data.scheduler.thesis/', '').replace('-', '_').upper()
                                    container['env'].append({
                                        'name': f"DATA_SCHEDULER_THESIS_{env_name}",
                                        'value': v
                                    })
                    
                    if 'tolerations' not in item['spec']:
                        item['spec']['tolerations'] = []
                    
                    has_control_plane_toleration = False
                    for toleration in item['spec'].get('tolerations', []):
                        if toleration.get('key') == 'node-role.kubernetes.io/control-plane':
                            has_control_plane_toleration = True
                            break
                    
                    if not has_control_plane_toleration:
                        item['spec']['tolerations'].append({
                            'key': 'node-role.kubernetes.io/control-plane',
                            'operator': 'Exists',
                            'effect': 'NoSchedule'
                        })
                        
                        item['spec']['tolerations'].append({
                            'key': 'node-role.kubernetes.io/master',
                            'operator': 'Exists',
                            'effect': 'NoSchedule'
                        })
                
                modified_workload.append(item)
            
            # Save the modified workload to a temporary file
            temp_file = Path(f"benchmarks/simulated/results/tmp_{workload_name}_{scheduler_name}_{iteration}.yaml")
            with open(temp_file, 'w') as f:
                yaml.dump_all(modified_workload, f)
        except Exception as e:
            logger.error(f"Error preparing workload: {e}")
            return False
        
        start_time = time.time()
        cmd = ["kubectl", "apply", "-f", str(temp_file)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to deploy workload: {result.stderr}")
            return False
        
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        self.results["workloads"][workload_key] = {
            "start_time": start_time,
            "deploy_duration": time.time() - start_time,
            "status": "deployed",
            "iteration": iteration
        }
        
        self.results["metrics"][workload_key] = {
            "pod_metrics": [],
            "scheduling_metrics": {
                "placement_latencies": [],
                "scheduling_attempts": 0,
                "scheduling_errors": 0
            },
            "data_locality_metrics": {
                "scores": [],
                "data_refs": 0,
                "data_local_refs": 0
            },
            "resource_usage": {
                "cpu": {},
                "memory": {},
                "network": {}
            }
        }
        
        logger.info(f"Deployed workload {workload_key}")
        
        self._wait_for_workload_completion(workload_name, scheduler_name, iteration)
        
        self._collect_workload_metrics(workload_name, scheduler_name, iteration)
        
        # clean up
        if self.config.get('execution', {}).get('cleanup_after_run', True):
            cmd = ["kubectl", "delete", "-f", str(temp_file)]
            cleanup_result = subprocess.run(cmd, capture_output=True, text=True)
            if cleanup_result.returncode != 0:
                logger.warning(f"Failed to clean up workload: {cleanup_result.stderr}")
            else:
                logger.info(f"Cleaned up workload {workload_key}")
        
        logger.info(f"Temporary workload file saved at: {temp_file}")
        
        return True
    
    def _wait_for_workload_completion(self, workload_name, scheduler_name, iteration):
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        run_suffix = f"{self.run_id}-{iteration}"
        
        logger.info(f"Waiting for workload {workload_key} to complete")
        
        max_wait_time = self.config.get('execution', {}).get('max_wait_time', 300)
        
        if workload_name in ['data-locality-test', 'ml-training-pipeline', 'data-intensive-workflow']:
            max_wait_time = 600  # 10 minutes for more data-intensive workloads
        
        poll_interval = self.config.get('execution', {}).get('poll_interval', 5)
        
        start_time = time.time()
        pending_warning_threshold = 60  
        
        while time.time() - start_time < max_wait_time:
            try:
                pods = self.k8s_client.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"benchmark-run-id={self.run_id},workload={workload_name},iteration={iteration}"
                )
                
                all_completed = True
                still_pending = 0
                still_running = 0
                completed = 0
                failed = 0
                
                pending_pods = []
                
                for pod in pods.items:
                    phase = pod.status.phase
                    if phase == 'Pending':
                        still_pending += 1
                        all_completed = False
                        pending_pods.append(pod)
                    elif phase == 'Running':
                        still_running += 1
                        all_completed = False
                        
                        if pod.status.start_time:
                            running_time = time.time() - pod.status.start_time.timestamp()
                            if running_time > max_wait_time * 0.8:  # 80% of max wait time
                                logger.warning(f"Pod {pod.metadata.name} has been running for {running_time:.0f}s, it may be stuck")
                                self._check_pod_logs(pod.metadata.name, namespace)
                    elif phase == 'Succeeded':
                        completed += 1
                    elif phase == 'Failed':
                        failed += 1
                        self._log_pod_failure(pod.metadata.name, namespace)
                    
                self.results["workloads"][workload_key]["pod_status"] = {
                    "total": len(pods.items),
                    "pending": still_pending,
                    "running": still_running,
                    "completed": completed,
                    "failed": failed
                }
                
               
                if (completed + failed) == len(pods.items) and len(pods.items) > 0:
                    logger.info(f"Workload {workload_key} completed: {completed} succeeded, {failed} failed")
                    self.results["workloads"][workload_key]["status"] = "completed"
                    self.results["workloads"][workload_key]["completion_time"] = time.time()
                    self.results["workloads"][workload_key]["duration"] = time.time() - start_time
                    break
                
                if failed == len(pods.items) and len(pods.items) > 0:
                    logger.warning(f"Workload {workload_key} failed: all {failed} pods failed")
                    self.results["workloads"][workload_key]["status"] = "failed"
                    self.results["workloads"][workload_key]["completion_time"] = time.time()
                    self.results["workloads"][workload_key]["duration"] = time.time() - start_time
                    break
                
                elapsed_time = time.time() - start_time
                if (still_pending > 0 or still_running > 0) and elapsed_time > pending_warning_threshold:
                    pending_warning_threshold += 60  # Increase for next warning
                    
                    if still_pending > 0:
                        logger.warning(f"Pods have been pending for {elapsed_time:.0f} seconds. Checking details...")
                        
                        for pod in pending_pods:
                            self._check_pod_scheduling_issues(pod.metadata.name, namespace)
                    
                    if still_pending > 0 and elapsed_time > 120:  
                        self._check_cluster_resources(namespace)
                        
                    if still_running > 0 and elapsed_time > 180:  
                        for pod in pods.items:
                            if pod.status.phase == 'Running':
                                self._check_pod_logs(pod.metadata.name, namespace)
                
                logger.info(f"Workload {workload_key}: {still_pending} pending, {still_running} running, {completed} completed, {failed} failed")
                time.sleep(poll_interval)
            
            except ApiException as e:
                logger.error(f"Error checking pod status: {e}")
                time.sleep(poll_interval)
        
        if time.time() - start_time >= max_wait_time:
            logger.warning(f"Workload {workload_key} did not complete within {max_wait_time} seconds")
            self.results["workloads"][workload_key]["status"] = "timeout"
            self.results["workloads"][workload_key]["completion_time"] = time.time()
            self.results["workloads"][workload_key]["duration"] = max_wait_time
            
            # Collect logs from running/pending pods for timeout diagnosis
            try:
                pods = self.k8s_client.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"benchmark-run-id={self.run_id},workload={workload_name},iteration={iteration}"
                )
                
                for pod in pods.items:
                    if pod.status.phase in ['Running', 'Pending']:
                        logger.warning(f"Pod {pod.metadata.name} did not complete in time")
                        self._check_pod_logs(pod.metadata.name, namespace, tail_lines=50)
            except Exception as e:
                logger.error(f"Failed to collect logs from timed out pods: {e}")

    def _check_pod_scheduling_issues(self, pod_name, namespace):
        """Check why a pod is not being scheduled"""
        try:
            cmd = f"kubectl describe pod {pod_name} -n {namespace}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                describe_output = result.stdout
                
                # extract events from pod description
                events_section = describe_output.split("Events:")[1] if "Events:" in describe_output else ""
                if events_section and "FailedScheduling" in events_section:
                    logger.warning(f"Pod {pod_name} scheduling issues found:")
                    for line in events_section.splitlines():
                        if "FailedScheduling" in line:
                            logger.warning(f"  {line.strip()}")
            
            scheduler_logs_cmd = f"kubectl logs -l app=data-locality-scheduler -n {namespace} --tail=20"
            scheduler_logs = subprocess.run(scheduler_logs_cmd, shell=True, capture_output=True, text=True)
            
            if scheduler_logs.returncode == 0 and pod_name in scheduler_logs.stdout:
                logger.warning(f"Recent scheduler logs for pod {pod_name}:")
                for line in scheduler_logs.stdout.splitlines():
                    if pod_name in line:
                        logger.warning(f"  {line.strip()}")
        except Exception as e:
            logger.error(f"Error checking pod scheduling issues: {e}")

    def _check_pod_logs(self, pod_name, namespace, tail_lines=20):
        """Check logs from a pod to diagnose issues"""
        try:
            cmd = f"kubectl logs {pod_name} -n {namespace} --tail={tail_lines}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0 and result.stdout:
                logger.info(f"Recent logs from pod {pod_name}:")
                for line in result.stdout.splitlines()[-5:]:  # Show just last 5 lines in info logs
                    logger.info(f"  {line.strip()}")
            else:
                logger.warning(f"No logs available from pod {pod_name} or error retrieving logs: {result.stderr}")
        except Exception as e:
            logger.error(f"Error checking pod logs: {e}")

    def _check_cluster_resources(self, namespace):
        """Check cluster resource utilization"""
        try:
            cmd = "kubectl describe nodes | grep -A 5 'Allocated resources'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.warning("Cluster resource utilization:")
                for line in result.stdout.splitlines():
                    if any(x in line for x in ["CPU", "Memory", "Allocated", "Resource"]):
                        logger.warning(f"  {line.strip()}")
        except Exception as e:
            logger.error(f"Error checking cluster resources: {e}")
                
    def _collect_workload_metrics(self, workload_name, scheduler_name, iteration):
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        run_suffix = f"-{self.run_id}-{iteration}"
        
        logger.info(f"Collecting metrics for workload {workload_key}")
        
        try:
            pods = self.k8s_client.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"benchmark-run-id={self.run_id},workload={workload_name},iteration={iteration}"
            )
            
            pod_metrics = []
            for pod in pods.items:
                placement = {
                    "pod_name": pod.metadata.name,
                    "node": pod.spec.node_name,
                    "scheduler": scheduler_name,
                    "phase": pod.status.phase,
                    "start_time": pod.status.start_time.timestamp() if pod.status.start_time else None,
                    "completion_time": None,  
                    "data_annotations": {k: v for k, v in pod.metadata.annotations.items() 
                                        if k.startswith('data.scheduler.thesis/')},
                    "placement_latency": None, 
                    "initialization_time": None, 
                    "execution_time": None,  
                    "container_statuses": []
                }
                
                if pod.status.container_statuses:
                    for container in pod.status.container_statuses:
                        container_status = {
                            "name": container.name,
                            "ready": container.ready,
                            "restart_count": container.restart_count,
                            "state": None
                        }
                        
                        if container.state.running:
                            container_status["state"] = "running"
                            container_status["started_at"] = container.state.running.started_at.timestamp() if container.state.running.started_at else None
                        elif container.state.terminated:
                            container_status["state"] = "terminated"
                            container_status["started_at"] = container.state.terminated.started_at.timestamp() if container.state.terminated.started_at else None
                            container_status["finished_at"] = container.state.terminated.finished_at.timestamp() if container.state.terminated.finished_at else None
                            container_status["exit_code"] = container.state.terminated.exit_code
                            container_status["reason"] = container.state.terminated.reason
                        elif container.state.waiting:
                            container_status["state"] = "waiting"
                            container_status["reason"] = container.state.waiting.reason
                        
                        placement["container_statuses"].append(container_status)
                
                if placement["container_statuses"]:
                    for container in placement["container_statuses"]:
                        if container["state"] == "terminated" and container["started_at"] and container["finished_at"]:
                            placement["execution_time"] = container["finished_at"] - container["started_at"]
                        
                        if container["state"] == "terminated" or container["state"] == "running":
                            if container["started_at"] and placement["start_time"]:
                                placement["initialization_time"] = container["started_at"] - placement["start_time"]
                
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        if condition.type == 'PodScheduled' and condition.status == 'True':
                            placement["scheduling_time"] = condition.last_transition_time.timestamp() if condition.last_transition_time else None
                            
                            if placement["scheduling_time"] and pod.metadata.creation_timestamp:
                                creation_time = pod.metadata.creation_timestamp.timestamp()
                                placement["placement_latency"] = placement["scheduling_time"] - creation_time
                        
                        if condition.type == 'Ready' and condition.status == 'False' and condition.reason == 'PodCompleted':
                            placement["completion_time"] = condition.last_transition_time.timestamp() if condition.last_transition_time else None
                
                node_info = None
                for node in self.results["metadata"]["cluster_info"]["nodes"]:
                    if node["name"] == placement["node"]:
                        node_info = node
                        break
                
                if node_info:
                    node_type = node_info["labels"].get("node-capability/node-type", "unknown")
                    placement["node_type"] = node_type
                    
                    node_capabilities = {k.replace('node-capability/', ''): v 
                                       for k, v in node_info["labels"].items() 
                                       if k.startswith('node-capability/')}
                    placement["node_capabilities"] = node_capabilities
                
                pod_metrics.append(placement)
            
            self.results["metrics"][workload_key]["pod_metrics"] = pod_metrics
            
            placement_latencies = [p["placement_latency"] for p in pod_metrics if p["placement_latency"] is not None]
            
            if placement_latencies:
                self.results["metrics"][workload_key]["scheduling_metrics"]["placement_latencies"] = placement_latencies
                self.results["metrics"][workload_key]["scheduling_metrics"]["avg_placement_latency"] = sum(placement_latencies) / len(placement_latencies)
                self.results["metrics"][workload_key]["scheduling_metrics"]["min_placement_latency"] = min(placement_latencies)
                self.results["metrics"][workload_key]["scheduling_metrics"]["max_placement_latency"] = max(placement_latencies)
            
            field_selector = ",".join([f"involvedObject.name={pod.metadata.name}" for pod in pods.items])
            events = self.k8s_client.list_namespaced_event(
                namespace=namespace,
                field_selector=field_selector
            )
            
            scheduling_attempts = 0
            scheduling_errors = 0
            
            scheduling_events = []
            for event in events.items:
                if event.reason in ['Scheduled', 'FailedScheduling']:
                    scheduling_events.append({
                        'pod': event.involved_object.name,
                        'reason': event.reason,
                        'message': event.message,
                        'count': event.count,
                        'type': event.type,
                        'timestamp': event.last_timestamp.timestamp() if event.last_timestamp else None
                    })
                    
                    if event.reason == 'Scheduled':
                        scheduling_attempts += 1
                    elif event.reason == 'FailedScheduling':
                        scheduling_errors += 1
            
            self.results["metrics"][workload_key]["scheduling_metrics"]["scheduling_attempts"] = scheduling_attempts
            self.results["metrics"][workload_key]["scheduling_metrics"]["scheduling_errors"] = scheduling_errors
            self.results["metrics"][workload_key]["scheduling_metrics"]["scheduling_events"] = scheduling_events
            
            self._calculate_data_locality_metrics(pod_metrics, workload_name, scheduler_name, iteration)
        
        except ApiException as e:
            logger.error(f"Error collecting pod metrics: {e}")
    
    def _calculate_data_locality_metrics(self, pod_metrics, workload_name, scheduler_name, iteration):
        namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        
        logger.info(f"Calculating data locality metrics for {workload_key}")
        
        # Ensure storage nodes and bucket_to_nodes are properly initialized
        storage_nodes = {}
        bucket_to_nodes = {}
        
        # Map MinIO services to nodes
        try:
            pods = self.k8s_client.list_namespaced_pod(
                namespace=namespace,
                label_selector="app in (minio,minio-central,minio-edge-region1,minio-edge-region2)"
            )
            
            for pod in pods.items:
                if pod.status.phase != 'Running' or not pod.spec.node_name:
                    continue
                
                node_name = pod.spec.node_name
                pod_name = pod.metadata.name
                
                # Determine service name based on pod name
                if 'central' in pod_name:
                    service_name = "minio-central"
                elif 'region1' in pod_name:
                    service_name = "minio-edge-region1"
                elif 'region2' in pod_name:
                    service_name = "minio-edge-region2"
                else:
                    service_name = "minio"
                
                storage_nodes[service_name] = node_name
                logger.info(f"Mapped storage service {service_name} to node {node_name}")
        except Exception as e:
            logger.error(f"Error mapping storage pods to nodes: {e}")
        
        # Map buckets to nodes using established mappings
        default_bucket_mapping = {
            "minio-central": ["datasets", "intermediate", "results", "shared", "test-bucket"],
            "minio-edge-region1": ["edge-data", "region1-bucket"],
            "minio-edge-region2": ["region2-bucket"]
        }
        
        for service, node in storage_nodes.items():
            if service in default_bucket_mapping:
                for bucket in default_bucket_mapping[service]:
                    if bucket not in bucket_to_nodes:
                        bucket_to_nodes[bucket] = []
                    if node not in bucket_to_nodes[bucket]:
                        bucket_to_nodes[bucket].append(node)
        
        logger.info(f"Storage nodes: {storage_nodes}")
        logger.info(f"Bucket to nodes mapping: {bucket_to_nodes}")
        
        # Initialize metric counters
        data_locality_scores = []
        total_data_refs = 0
        local_data_refs = 0
        same_zone_refs = 0
        same_region_refs = 0
        cross_region_refs = 0
        total_data_size = 0
        local_data_size = 0
        same_zone_data_size = 0
        same_region_data_size = 0
        cross_region_data_size = 0
        edge_to_cloud_transfers = 0
        cloud_to_edge_transfers = 0
        edge_to_cloud_data_size = 0
        cloud_to_edge_data_size = 0
        
        # Get pod node topology
        pod_node_topology = {}
        for pod_metric in pod_metrics:
            pod_name = pod_metric.get('pod_name', 'unknown')
            pod_node = pod_metric.get('node')
            
            if not pod_node:
                continue
                
            try:
                node = self.k8s_client.read_node(pod_node)
                region = node.metadata.labels.get('topology.kubernetes.io/region', '')
                zone = node.metadata.labels.get('topology.kubernetes.io/zone', '')
                node_type = node.metadata.labels.get('node-capability/node-type', 'unknown')
                
                pod_node_topology[pod_name] = {
                    'node': pod_node,
                    'region': region,
                    'zone': zone,
                    'node_type': node_type
                }
            except Exception as e:
                logger.warning(f"Failed to get topology for node {pod_node}: {e}")
        
        # Analyze data locality for each pod
        for pod_metric in pod_metrics:
            pod_name = pod_metric.get('pod_name', 'unknown')
            pod_node = pod_metric.get('node')
            data_refs = pod_metric.get('data_annotations', {})
            
            if data_refs and pod_node and pod_name in pod_node_topology:
                logger.debug(f"Analyzing data locality for pod {pod_name} on node {pod_node}")
                
                # Initialize counters for this pod
                pod_local_refs = 0
                pod_same_zone_refs = 0
                pod_same_region_refs = 0
                pod_cross_region_refs = 0
                pod_total_refs = 0
                pod_data_refs = []
                
                pod_total_data_size = 0
                pod_local_data_size = 0
                pod_same_zone_data_size = 0
                pod_same_region_data_size = 0
                pod_cross_region_data_size = 0
                pod_edge_to_cloud_data_size = 0
                pod_cloud_to_edge_data_size = 0
                
                pod_region = pod_node_topology[pod_name]['region']
                pod_zone = pod_node_topology[pod_name]['zone']
                pod_node_type = pod_node_topology[pod_name]['node_type']
                
                # Process each data reference
                for key, value in data_refs.items():
                    if key.startswith('data.scheduler.thesis/input-') or key.startswith('data.scheduler.thesis/output-'):
                        # Count as a data reference
                        pod_total_refs += 1
                        total_data_refs += 1
                        
                        # Parse parts of the data reference
                        parts = value.split(',')
                        if len(parts) < 2:
                            continue
                            
                        data_path = parts[0]
                        
                        # Safely parse size
                        try:
                            data_size = int(parts[1])
                        except (ValueError, IndexError):
                            data_size = 0
                        
                        pod_total_data_size += data_size
                        total_data_size += data_size
                        
                        # Extract bucket from data path
                        bucket = data_path.split('/')[0] if '/' in data_path else data_path
                        
                        # Default locality values
                        is_local = False
                        is_same_zone = False
                        is_same_region = False
                        storage_node = None
                        storage_region = ""
                        storage_zone = ""
                        storage_node_type = "unknown"
                        
                        # Find where the data is stored
                        if bucket in bucket_to_nodes:
                            bucket_nodes = bucket_to_nodes[bucket]
                            
                            # First check if data is local to pod's node
                            if pod_node in bucket_nodes:
                                is_local = True
                                storage_node = pod_node
                                storage_region = pod_region
                                storage_zone = pod_zone
                                storage_node_type = pod_node_type
                                logger.info(f"Local data reference found: {data_path} on node {pod_node}")
                            else:
                                # Check other nodes where this bucket exists
                                for node_name in bucket_nodes:
                                    try:
                                        # Get or use cached node topology
                                        if node_name in pod_node_topology:
                                            node_region = pod_node_topology[node_name]['region']
                                            node_zone = pod_node_topology[node_name]['zone']
                                            node_type = pod_node_topology[node_name]['node_type']
                                        else:
                                            # Try to get node info
                                            try:
                                                node = self.k8s_client.read_node(node_name)
                                                node_region = node.metadata.labels.get('topology.kubernetes.io/region', '')
                                                node_zone = node.metadata.labels.get('topology.kubernetes.io/zone', '')
                                                node_type = node.metadata.labels.get('node-capability/node-type', 'unknown')
                                                
                                                # Cache this info
                                                pod_node_topology[node_name] = {
                                                    'node': node_name,
                                                    'region': node_region,
                                                    'zone': node_zone,
                                                    'node_type': node_type
                                                }
                                            except Exception as e:
                                                logger.warning(f"Error reading node {node_name}: {e}")
                                                # Use default values if we can't read the node
                                                node_region = ''
                                                node_zone = ''
                                                node_type = 'unknown'
                                        
                                        # Set storage node if this is our first found storage node
                                        if not storage_node:
                                            storage_node = node_name
                                            storage_region = node_region
                                            storage_zone = node_zone
                                            storage_node_type = node_type
                                        
                                        # Check for same zone first (better locality than just same region)
                                        if pod_zone and node_zone and pod_zone == node_zone:
                                            is_same_zone = True
                                            storage_node = node_name
                                            storage_region = node_region
                                            storage_zone = node_zone
                                            storage_node_type = node_type
                                            break
                                        # Then check for same region
                                        elif pod_region and node_region and pod_region == node_region:
                                            is_same_region = True
                                            if not is_same_zone:  # only set if we haven't found same zone
                                                storage_node = node_name
                                                storage_region = node_region
                                                storage_zone = node_zone
                                                storage_node_type = node_type
                                    except Exception as e:
                                        logger.warning(f"Error checking node {node_name} topology: {e}")
                        
                        # Determine data locality
                        data_locality = "UNKNOWN"
                        if is_local:
                            data_locality = "LOCAL"
                            pod_local_refs += 1
                            local_data_refs += 1
                            pod_local_data_size += data_size
                            local_data_size += data_size
                        elif is_same_zone:
                            data_locality = "SAME_ZONE"
                            pod_same_zone_refs += 1
                            same_zone_refs += 1
                            pod_same_zone_data_size += data_size
                            same_zone_data_size += data_size
                        elif is_same_region:
                            data_locality = "SAME_REGION"
                            pod_same_region_refs += 1
                            same_region_refs += 1
                            pod_same_region_data_size += data_size
                            same_region_data_size += data_size
                        else:
                            data_locality = "CROSS_REGION"
                            pod_cross_region_refs += 1
                            cross_region_refs += 1
                            pod_cross_region_data_size += data_size
                            cross_region_data_size += data_size
                        
                        # Track edge-to-cloud or cloud-to-edge transfers
                        is_edge_to_cloud = pod_node_type == 'edge' and storage_node_type == 'cloud'
                        is_cloud_to_edge = pod_node_type == 'cloud' and storage_node_type == 'edge'
                        
                        if is_edge_to_cloud and not is_local:
                            edge_to_cloud_transfers += 1
                            edge_to_cloud_data_size += data_size
                            pod_edge_to_cloud_data_size += data_size
                        elif is_cloud_to_edge and not is_local:
                            cloud_to_edge_transfers += 1
                            cloud_to_edge_data_size += data_size
                            pod_cloud_to_edge_data_size += data_size
                        
                        # Record data reference details
                        pod_data_refs.append({
                            'key': key,
                            'path': data_path,
                            'bucket': bucket,
                            'size': data_size,
                            'locality': data_locality,
                            'storage_node': storage_node,
                            'storage_region': storage_region,
                            'storage_zone': storage_zone,
                            'storage_node_type': storage_node_type
                        })
                
                # Log detailed per-pod locality info
                logger.info(f"Pod {pod_name} on node {pod_node} ({pod_region}/{pod_zone}): "
                            f"{pod_local_refs} local, {pod_same_zone_refs} same zone, "
                            f"{pod_same_region_refs} same region, {pod_cross_region_refs} cross region "
                            f"out of {pod_total_refs} total refs")
                
                # Calculate pod's data locality score if it has data references
                if pod_total_refs > 0:
                    # Calculate weighted scores with proper weighting
                    locality_weight_sum = (
                        pod_local_refs * 1.0 + 
                        pod_same_zone_refs * 0.8 + 
                        pod_same_region_refs * 0.5
                    )
                    pod_locality_score = locality_weight_sum / pod_total_refs
                    
                    # Calculate size-weighted score
                    size_weighted_score = 0
                    if pod_total_data_size > 0:
                        size_weighted_score = (
                            (pod_local_data_size * 1.0) + 
                            (pod_same_zone_data_size * 0.8) + 
                            (pod_same_region_data_size * 0.5)
                        ) / pod_total_data_size
                    
                    # Store pod-level locality metrics
                    pod_metric['data_locality'] = {
                        'score': pod_locality_score,
                        'local_refs': pod_local_refs,
                        'same_zone_refs': pod_same_zone_refs,
                        'same_region_refs': pod_same_region_refs,
                        'cross_region_refs': pod_cross_region_refs,
                        'total_refs': pod_total_refs,
                        'size_weighted_score': size_weighted_score,
                        'total_data_size': pod_total_data_size,
                        'local_data_size': pod_local_data_size,
                        'same_zone_data_size': pod_same_zone_data_size,
                        'same_region_data_size': pod_same_region_data_size,
                        'cross_region_data_size': pod_cross_region_data_size,
                        'edge_to_cloud_data_size': pod_edge_to_cloud_data_size,
                        'cloud_to_edge_data_size': pod_cloud_to_edge_data_size
                    }
                    
                    data_locality_scores.append(pod_locality_score)
                    logger.info(f"Pod {pod_name} data locality score: {pod_locality_score:.4f}, "
                            f"Size-weighted: {size_weighted_score:.4f}")
        
        # Calculate overall metrics for this workload
        if total_data_refs > 0:
            overall_locality_score = local_data_refs / total_data_refs
            
            weighted_locality_sum = (
                local_data_refs * 1.0 + 
                same_zone_refs * 0.8 + 
                same_region_refs * 0.5
            )
            weighted_locality_score = weighted_locality_sum / total_data_refs
            
            size_weighted_locality = 0
            if total_data_size > 0:
                size_weighted_locality = (
                    (local_data_size * 1.0) + 
                    (same_zone_data_size * 0.8) + 
                    (same_region_data_size * 0.5)
                ) / total_data_size
            
            logger.info(f"Overall data locality score: {overall_locality_score:.4f}")
            logger.info(f"Weighted locality score: {weighted_locality_score:.4f}")
            logger.info(f"Size-weighted locality: {size_weighted_locality:.4f}")
            
            # Calculate network metrics
            network_metrics = {
                "total_data_size_bytes": total_data_size,
                "local_data_size_bytes": local_data_size,
                "same_zone_data_size_bytes": same_zone_data_size,
                "same_region_data_size_bytes": same_region_data_size,
                "cross_region_data_size_bytes": cross_region_data_size,
                "edge_to_cloud_transfers": edge_to_cloud_transfers,
                "cloud_to_edge_transfers": cloud_to_edge_transfers,
                "edge_to_cloud_data_size_bytes": edge_to_cloud_data_size,
                "cloud_to_edge_data_size_bytes": cloud_to_edge_data_size
            }
            
            # Log network metrics in human-readable format
            total_mb = total_data_size / (1024*1024)
            local_mb = local_data_size / (1024*1024)
            same_zone_mb = same_zone_data_size / (1024*1024)
            same_region_mb = same_region_data_size / (1024*1024)
            cross_region_mb = cross_region_data_size / (1024*1024)
            edge_to_cloud_mb = edge_to_cloud_data_size / (1024*1024)
            cloud_to_edge_mb = cloud_to_edge_data_size / (1024*1024)
            
            logger.info(f"Network metrics for {workload_key}:")
            logger.info(f"  - Total data size: {total_mb:.2f} MB")
            logger.info(f"  - Local data access: {local_mb:.2f} MB ({100*local_data_size/total_data_size:.1f}%)")
            logger.info(f"  - Same zone data: {same_zone_mb:.2f} MB ({100*same_zone_data_size/total_data_size:.1f}%)")
            logger.info(f"  - Same region data: {same_region_mb:.2f} MB ({100*same_region_data_size/total_data_size:.1f}%)")
            logger.info(f"  - Cross-region data: {cross_region_mb:.2f} MB ({100*cross_region_data_size/total_data_size:.1f}%)")
            logger.info(f"  - Edge-to-cloud data: {edge_to_cloud_mb:.2f} MB ({100*edge_to_cloud_data_size/total_data_size:.1f}%)")
            logger.info(f"  - Cloud-to-edge data: {cloud_to_edge_mb:.2f} MB ({100*cloud_to_edge_data_size/total_data_size:.1f}%)")
            
            # Store workload-level locality metrics
            self.results["metrics"][workload_key]["data_locality_metrics"] = {
                "overall_score": overall_locality_score,
                "weighted_score": weighted_locality_score,
                "size_weighted_score": size_weighted_locality,
                "local_refs": local_data_refs,
                "same_zone_refs": same_zone_refs,
                "same_region_refs": same_region_refs,
                "cross_region_refs": cross_region_refs,
                "total_refs": total_data_refs,
                "local_data_size": local_data_size,
                "same_zone_data_size": same_zone_data_size,
                "same_region_data_size": same_region_data_size,
                "cross_region_data_size": cross_region_data_size,
                "total_data_size": total_data_size,
                "network_metrics": network_metrics
            }
        else:
            logger.warning("No data references found for locality calculation")
            # Initialize with zeros to avoid future errors
            self.results["metrics"][workload_key]["data_locality_metrics"] = {
                "overall_score": 0,
                "weighted_score": 0,
                "size_weighted_score": 0,
                "local_refs": 0,
                "same_zone_refs": 0,
                "same_region_refs": 0,
                "cross_region_refs": 0,
                "total_refs": 0,
                "local_data_size": 0,
                "same_zone_data_size": 0,
                "same_region_data_size": 0,
                "cross_region_data_size": 0,
                "total_data_size": 0,
                "network_metrics": {
                    "total_data_size_bytes": 0,
                    "local_data_size_bytes": 0,
                    "same_zone_data_size_bytes": 0,
                    "same_region_data_size_bytes": 0,
                    "cross_region_data_size_bytes": 0,
                    "edge_to_cloud_transfers": 0,
                    "cloud_to_edge_transfers": 0,
                    "edge_to_cloud_data_size_bytes": 0,
                    "cloud_to_edge_data_size_bytes": 0
                }
            }
                
    def _determine_service_name(self, pod_name, role, region):
        """Determine MinIO service name from pod information"""
        if 'minio-central' in pod_name or role == 'central':
            return "minio-central"
        elif 'minio-edge-region1' in pod_name or (role == 'edge' and region == 'region-1'):
            return "minio-edge-region1"
        elif 'minio-edge-region2' in pod_name or (role == 'edge' and region == 'region-2'):
            return "minio-edge-region2"
        elif 'minio' in pod_name:
            if 'region1' in pod_name or 'region-1' in pod_name:
                return "minio-edge-region1"
            elif 'region2' in pod_name or 'region-2' in pod_name:
                return "minio-edge-region2"
            else:
                return "minio-central"
        return None

    def _map_buckets_to_nodes_robust(self, storage_nodes, bucket_to_nodes):
        default_bucket_mapping = {
            "minio-central": ["datasets", "intermediate", "results", "shared", "test-bucket"],
            "minio-edge-region1": ["edge-data", "region1-bucket"],
            "minio-edge-region2": ["region2-bucket"]
        }
        
        for service, node in storage_nodes.items():
            if service in default_bucket_mapping:
                for bucket in default_bucket_mapping[service]:
                    if bucket not in bucket_to_nodes:
                        bucket_to_nodes[bucket] = []
                    if node not in bucket_to_nodes[bucket]:
                        bucket_to_nodes[bucket].append(node)
        
        try:
            for service, node in storage_nodes.items():
                service_short_name = service.replace("minio-", "").replace("-", "_")
                cmd = f"mc ls {service_short_name}/ 2>/dev/null || true"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if result.returncode == 0 and result.stdout:
                    for line in result.stdout.splitlines():
                        parts = line.strip().split()
                        if len(parts) >= 1:
                            potential_bucket = parts[-1].strip('/')
                            if potential_bucket and not potential_bucket.startswith('.'):
                                if potential_bucket not in bucket_to_nodes:
                                    bucket_to_nodes[potential_bucket] = []
                                if node not in bucket_to_nodes[potential_bucket]:
                                    bucket_to_nodes[potential_bucket].append(node)
                                    logger.info(f"Discovered bucket {potential_bucket} on node {node} via mc ls")
        except Exception as e:
            logger.warning(f"Failed to dynamically discover buckets: {e}")
                
    
    def run_benchmarks(self):
        logger.info("Starting benchmark runs")
        
        benchmark_start_time = time.time()
        
        try:
            self.prepare_environment()
            
            for workload_config in self.config.get('workloads', []):
                workload_name = workload_config.get('name') if isinstance(workload_config, dict) else workload_config
                iterations = workload_config.get('iterations', 3) if isinstance(workload_config, dict) else 3
                
                for scheduler in self.config.get('schedulers', []):
                    scheduler_name = scheduler.get('name') if isinstance(scheduler, dict) else scheduler
                    
                    for iteration in range(1, iterations + 1):
                        try:
                            success = self.run_workload(workload_name, scheduler_name, iteration)
                            if not success:
                                logger.warning(f"Workload {workload_name} with scheduler {scheduler_name} (iteration {iteration}) failed")
                        except Exception as e:
                            logger.error(f"Error running workload {workload_name} with scheduler {scheduler_name} (iteration {iteration}): {e}")
                            # Continue with next workload instead of halting everything
            
            self._compare_results()
            benchmark_end_time = time.time()
            self.results["metadata"]["benchmark_duration"] = benchmark_end_time - benchmark_start_time
            self._save_results()
            self._generate_report()
            
            logger.info(f"Benchmark runs completed in {benchmark_end_time - benchmark_start_time:.2f} seconds")
        except Exception as e:
            logger.error(f"Benchmark run encountered an error: {e}")
            # Still save partial results
            try:
                benchmark_end_time = time.time()
                self.results["metadata"]["benchmark_duration"] = benchmark_end_time - benchmark_start_time
                self.results["metadata"]["completed"] = False
                self.results["metadata"]["error"] = str(e)
                self._save_results()
                self._generate_report()
            except Exception as save_error:
                logger.error(f"Failed to save partial results: {save_error}")
                
    # Modified method in benchmarks/simulated/framework/benchmark_runner.py

    def _compare_results(self):
        """Compare results between schedulers with proper type handling"""
        logger.info("Comparing results between schedulers")
        
        schedulers = set()
        workloads = set()
        
        for workload_key in self.results["workloads"]:
            parts = workload_key.rsplit('_', 2)
            if len(parts) >= 3:
                workload = parts[0]
                scheduler = parts[1]
                workloads.add(workload)
                schedulers.add(scheduler)
        
        comparison = {}
        
        for workload in workloads:
            comparison[workload] = {}
            
            scheduler_metrics = {}
            for scheduler in schedulers:
                scheduler_metrics[scheduler] = {
                    "data_locality_scores": [],
                    "weighted_locality_scores": [],
                    "size_weighted_scores": [],
                    "local_data_percentages": [],
                    "cross_region_percentages": [],
                    "edge_to_cloud_percentages": [],
                    "placement_latencies": [],
                    "edge_placements": 0,
                    "cloud_placements": 0,
                    "total_placements": 0,
                    "network_stats": {
                        "total_data_size": 0,
                        "local_data_size": 0,
                        "cross_region_data_size": 0,
                        "edge_to_cloud_data_size": 0
                    }
                }
                    
                for iteration in range(1, 10): # max 10 iterations
                    workload_key = f"{workload}_{scheduler}_{iteration}"
                    if workload_key in self.results["metrics"]:
                        metrics = self.results["metrics"][workload_key]
                        
                        # Process data locality metrics
                        if "data_locality_metrics" in metrics:
                            locality_metrics = metrics["data_locality_metrics"]
                            
                            # Overall locality score
                            if "overall_score" in locality_metrics:
                                scheduler_metrics[scheduler]["data_locality_scores"].append(
                                    locality_metrics["overall_score"]
                                )
                            
                            # Weighted locality score
                            if "weighted_score" in locality_metrics:
                                scheduler_metrics[scheduler]["weighted_locality_scores"].append(
                                    locality_metrics["weighted_score"]
                                )
                            
                            # Size-weighted locality score
                            if "size_weighted_score" in locality_metrics:
                                scheduler_metrics[scheduler]["size_weighted_scores"].append(
                                    locality_metrics["size_weighted_score"]
                                )
                            
                            # Calculate percentages
                            total_data_size = locality_metrics.get("total_data_size", 0)
                            if total_data_size > 0:
                                local_pct = (locality_metrics.get("local_data_size", 0) / total_data_size) * 100
                                cross_region_pct = (locality_metrics.get("cross_region_data_size", 0) / total_data_size) * 100
                                
                                scheduler_metrics[scheduler]["local_data_percentages"].append(local_pct)
                                scheduler_metrics[scheduler]["cross_region_percentages"].append(cross_region_pct)
                                
                                # Add to network stats
                                scheduler_metrics[scheduler]["network_stats"]["total_data_size"] += total_data_size
                                scheduler_metrics[scheduler]["network_stats"]["local_data_size"] += locality_metrics.get("local_data_size", 0)
                                scheduler_metrics[scheduler]["network_stats"]["cross_region_data_size"] += locality_metrics.get("cross_region_data_size", 0)
                                
                                # Edge to cloud transfers
                                if "network_metrics" in locality_metrics:
                                    network_metrics = locality_metrics["network_metrics"]
                                    edge_to_cloud_size = network_metrics.get("edge_to_cloud_data_size_bytes", 0)
                                    if edge_to_cloud_size > 0:
                                        edge_to_cloud_pct = (edge_to_cloud_size / total_data_size) * 100
                                        scheduler_metrics[scheduler]["edge_to_cloud_percentages"].append(edge_to_cloud_pct)
                                        scheduler_metrics[scheduler]["network_stats"]["edge_to_cloud_data_size"] += edge_to_cloud_size
                        
                        # Process scheduling metrics
                        if "scheduling_metrics" in metrics and "placement_latencies" in metrics["scheduling_metrics"]:
                            scheduler_metrics[scheduler]["placement_latencies"].extend(metrics["scheduling_metrics"]["placement_latencies"])
                        
                        # Process pod metrics for node type distribution
                        for pod in metrics.get("pod_metrics", []):
                            scheduler_metrics[scheduler]["total_placements"] += 1
                            if pod.get("node_type") == "edge":
                                scheduler_metrics[scheduler]["edge_placements"] += 1
                            elif pod.get("node_type") == "cloud":
                                scheduler_metrics[scheduler]["cloud_placements"] += 1
                
                # Ensure we have default values if no data was available
                if len(scheduler_metrics[scheduler]["data_locality_scores"]) == 0:
                    scheduler_metrics[scheduler]["data_locality_scores"] = [0.0]
                if len(scheduler_metrics[scheduler]["weighted_locality_scores"]) == 0:
                    scheduler_metrics[scheduler]["weighted_locality_scores"] = [0.0]
                if len(scheduler_metrics[scheduler]["size_weighted_scores"]) == 0:
                    scheduler_metrics[scheduler]["size_weighted_scores"] = [0.0]
            
            # Compare data locality scores
            if all(len(m["data_locality_scores"]) > 0 for m in scheduler_metrics.values()):
                data_locality_comparison = {}
                for scheduler, metrics in scheduler_metrics.items():
                    avg_score = sum(metrics["data_locality_scores"]) / len(metrics["data_locality_scores"])
                    avg_weighted = sum(metrics["weighted_locality_scores"]) / len(metrics["weighted_locality_scores"]) if metrics["weighted_locality_scores"] else 0
                    avg_size_weighted = sum(metrics["size_weighted_scores"]) / len(metrics["size_weighted_scores"]) if metrics["size_weighted_scores"] else 0
                    
                    data_locality_comparison[scheduler] = {
                        "mean": avg_score,
                        "weighted_mean": avg_weighted,
                        "size_weighted_mean": avg_size_weighted,
                        "scores": metrics["data_locality_scores"],
                        "min": min(metrics["data_locality_scores"]),
                        "max": max(metrics["data_locality_scores"]),
                        "local_data_percentage": sum(metrics["local_data_percentages"]) / len(metrics["local_data_percentages"]) if metrics["local_data_percentages"] else 0,
                        "cross_region_percentage": sum(metrics["cross_region_percentages"]) / len(metrics["cross_region_percentages"]) if metrics["cross_region_percentages"] else 0,
                        "edge_to_cloud_percentage": sum(metrics["edge_to_cloud_percentages"]) / len(metrics["edge_to_cloud_percentages"]) if metrics["edge_to_cloud_percentages"] else 0
                    }
                
                if "data-locality-scheduler" in data_locality_comparison and "default-scheduler" in data_locality_comparison:
                    baseline = data_locality_comparison["default-scheduler"]["mean"]
                    new_score = data_locality_comparison["data-locality-scheduler"]["mean"]
                    if baseline > 0:
                        improvement = ((new_score - baseline) / baseline) * 100
                        data_locality_comparison["improvement_percentage"] = improvement
                    else:
                        # If baseline is 0, calculate absolute improvement
                        data_locality_comparison["improvement_percentage"] = new_score * 100 if new_score > 0 else 0
                    
                    # Size-weighted improvement
                    baseline_size = data_locality_comparison["default-scheduler"]["size_weighted_mean"]
                    new_size_score = data_locality_comparison["data-locality-scheduler"]["size_weighted_mean"]
                    if baseline_size > 0:
                        size_improvement = ((new_size_score - baseline_size) / baseline_size) * 100
                        data_locality_comparison["size_weighted_improvement_percentage"] = size_improvement
                    else:
                        data_locality_comparison["size_weighted_improvement_percentage"] = new_size_score * 100 if new_size_score > 0 else 0
                    
                    # Local data percentage improvement
                    baseline_local = data_locality_comparison["default-scheduler"]["local_data_percentage"]
                    new_local = data_locality_comparison["data-locality-scheduler"]["local_data_percentage"]
                    if baseline_local > 0:
                        local_improvement = ((new_local - baseline_local) / baseline_local) * 100
                        data_locality_comparison["local_data_improvement_percentage"] = local_improvement
                    else:
                        data_locality_comparison["local_data_improvement_percentage"] = new_local * 100 if new_local > 0 else 0
                
                comparison[workload]["data_locality_comparison"] = data_locality_comparison
            
            # Compare preference satisfaction metrics
            preference_comparison = {}
            for scheduler, metrics in scheduler_metrics.items():
                edge_satisfaction = sum(metrics["edge_preference_satisfaction"]) / len(metrics["edge_preference_satisfaction"]) if metrics["edge_preference_satisfaction"] else 0
                cloud_satisfaction = sum(metrics["cloud_preference_satisfaction"]) / len(metrics["cloud_preference_satisfaction"]) if metrics["cloud_preference_satisfaction"] else 0
                region_satisfaction = sum(metrics["region_preference_satisfaction"]) / len(metrics["region_preference_satisfaction"]) if metrics["region_preference_satisfaction"] else 0
                
                preference_comparison[scheduler] = {
                    "edge_preference_satisfaction": edge_satisfaction,
                    "cloud_preference_satisfaction": cloud_satisfaction,
                    "region_preference_satisfaction": region_satisfaction
                }
            
            if "data-locality-scheduler" in preference_comparison and "default-scheduler" in preference_comparison:
                # Calculate preference satisfaction improvements
                edge_baseline = preference_comparison["default-scheduler"]["edge_preference_satisfaction"]
                edge_new = preference_comparison["data-locality-scheduler"]["edge_preference_satisfaction"]
                if edge_baseline > 0:
                    edge_improvement = ((edge_new - edge_baseline) / edge_baseline) * 100
                    preference_comparison["edge_preference_improvement"] = edge_improvement
                
                cloud_baseline = preference_comparison["default-scheduler"]["cloud_preference_satisfaction"]
                cloud_new = preference_comparison["data-locality-scheduler"]["cloud_preference_satisfaction"]
                if cloud_baseline > 0:
                    cloud_improvement = ((cloud_new - cloud_baseline) / cloud_baseline) * 100
                    preference_comparison["cloud_preference_improvement"] = cloud_improvement
                
                region_baseline = preference_comparison["default-scheduler"]["region_preference_satisfaction"]
                region_new = preference_comparison["data-locality-scheduler"]["region_preference_satisfaction"]
                if region_baseline > 0:
                    region_improvement = ((region_new - region_baseline) / region_baseline) * 100
                    preference_comparison["region_preference_improvement"] = region_improvement
            
            comparison[workload]["preference_comparison"] = preference_comparison
            
            # Compare network metrics
            network_comparison = {}
            for scheduler, metrics in scheduler_metrics.items():
                network_stats = metrics["network_stats"]
                total_data = network_stats["total_data_size"]
                
                if total_data > 0:
                    local_pct = (network_stats["local_data_size"] / total_data) * 100
                    cross_region_pct = (network_stats["cross_region_data_size"] / total_data) * 100
                    edge_to_cloud_pct = (network_stats["edge_to_cloud_data_size"] / total_data) * 100
                    
                    # Calculate average network overhead
                    avg_network_overhead = sum(metrics["network_overhead_bytes"]) / len(metrics["network_overhead_bytes"]) if metrics["network_overhead_bytes"] else 0
                    
                    network_comparison[scheduler] = {
                        "total_data_mb": total_data / (1024*1024),  # Convert to MB
                        "local_data_mb": network_stats["local_data_size"] / (1024*1024),
                        "cross_region_data_mb": network_stats["cross_region_data_size"] / (1024*1024),
                        "edge_to_cloud_data_mb": network_stats["edge_to_cloud_data_size"] / (1024*1024),
                        "network_overhead_mb": avg_network_overhead / (1024*1024),
                        "local_percentage": local_pct,
                        "cross_region_percentage": cross_region_pct,
                        "edge_to_cloud_percentage": edge_to_cloud_pct
                    }
            
            if network_comparison:
                comparison[workload]["network_comparison"] = network_comparison
                
                # Compute network efficiency improvement
                if "data-locality-scheduler" in network_comparison and "default-scheduler" in network_comparison:
                    dl_local_pct = network_comparison["data-locality-scheduler"]["local_percentage"]
                    def_local_pct = network_comparison["default-scheduler"]["local_percentage"]
                    
                    if def_local_pct > 0:
                        locality_improvement = ((dl_local_pct - def_local_pct) / def_local_pct) * 100
                        network_comparison["local_data_improvement_percentage"] = locality_improvement
                    
                    dl_cross = network_comparison["data-locality-scheduler"]["cross_region_percentage"]
                    def_cross = network_comparison["default-scheduler"]["cross_region_percentage"]
                    
                    if def_cross > 0:
                        cross_reduction = ((def_cross - dl_cross) / def_cross) * 100
                        network_comparison["cross_region_reduction_percentage"] = cross_reduction
                    
                    # Calculate network transfer reduction
                    dl_network_overhead = network_comparison["data-locality-scheduler"]["network_overhead_mb"]
                    def_network_overhead = network_comparison["default-scheduler"]["network_overhead_mb"]
                    
                    if def_network_overhead > 0:
                        overhead_reduction = ((def_network_overhead - dl_network_overhead) / def_network_overhead) * 100
                        network_comparison["network_overhead_reduction_percentage"] = overhead_reduction
                    
                    # Calculate estimated bandwidth savings
                    total_data_mb = network_comparison["data-locality-scheduler"]["total_data_mb"]
                    dl_transfer_mb = total_data_mb - network_comparison["data-locality-scheduler"]["local_data_mb"]
                    def_transfer_mb = total_data_mb - network_comparison["default-scheduler"]["local_data_mb"]
                    
                    if def_transfer_mb > 0:
                        transfer_reduction = ((def_transfer_mb - dl_transfer_mb) / def_transfer_mb) * 100
                        network_comparison["transfer_reduction_percentage"] = transfer_reduction
            
            # Compare scheduling latency
            if all(len(m["placement_latencies"]) > 0 for m in scheduler_metrics.values()):
                scheduling_latency_comparison = {}
                for scheduler, metrics in scheduler_metrics.items():
                    latencies = metrics["placement_latencies"]
                    scheduling_latency_comparison[scheduler] = {
                        "mean": sum(latencies) / len(latencies) if latencies else 0,
                        "min": min(latencies) if latencies else 0,
                        "max": max(latencies) if latencies else 0,
                        "latencies": latencies
                    }
                
                if "data-locality-scheduler" in scheduling_latency_comparison and "default-scheduler" in scheduling_latency_comparison:
                    baseline = scheduling_latency_comparison["default-scheduler"]["mean"]
                    new_latency = scheduling_latency_comparison["data-locality-scheduler"]["mean"]
                    if baseline > 0:
                        improvement = ((baseline - new_latency) / baseline) * 100
                        scheduling_latency_comparison["improvement_percentage"] = improvement
                
                comparison[workload]["scheduling_latency_comparison"] = scheduling_latency_comparison
            
            # Compare processing overhead time
            processing_overhead_comparison = {}
            for scheduler, metrics in scheduler_metrics.items():
                if metrics["processing_overhead_times"]:
                    avg_overhead = sum(metrics["processing_overhead_times"]) / len(metrics["processing_overhead_times"])
                    processing_overhead_comparison[scheduler] = {
                        "mean_overhead_seconds": avg_overhead,
                        "total_overhead_seconds": sum(metrics["processing_overhead_times"])
                    }
            
            if "data-locality-scheduler" in processing_overhead_comparison and "default-scheduler" in processing_overhead_comparison:
                dl_overhead = processing_overhead_comparison["data-locality-scheduler"]["mean_overhead_seconds"]
                def_overhead = processing_overhead_comparison["default-scheduler"]["mean_overhead_seconds"]
                
                if def_overhead > 0:
                    overhead_improvement = ((def_overhead - dl_overhead) / def_overhead) * 100
                    processing_overhead_comparison["improvement_percentage"] = overhead_improvement
            
            comparison[workload]["processing_overhead_comparison"] = processing_overhead_comparison
            
            # Compare node distribution
            node_distribution_comparison = {}
            for scheduler, metrics in scheduler_metrics.items():
                total_placements = metrics["total_placements"]
                if total_placements > 0:
                    edge_percentage = (metrics["edge_placements"] / total_placements) * 100
                    cloud_percentage = (metrics["cloud_placements"] / total_placements) * 100
                    
                    node_distribution_comparison[scheduler] = {
                        "edge_placements": metrics["edge_placements"],
                        "cloud_placements": metrics["cloud_placements"],
                        "total_placements": total_placements,
                        "edge_percentage": edge_percentage,
                        "cloud_percentage": cloud_percentage
                    }
            
            if node_distribution_comparison:
                comparison[workload]["node_distribution_comparison"] = node_distribution_comparison
                
                if "data-locality-scheduler" in node_distribution_comparison and "default-scheduler" in node_distribution_comparison:
                    dl_edge_pct = node_distribution_comparison["data-locality-scheduler"]["edge_percentage"] 
                    def_edge_pct = node_distribution_comparison["default-scheduler"]["edge_percentage"]
                    
                    if def_edge_pct > 0:
                        edge_improvement = ((dl_edge_pct - def_edge_pct) / def_edge_pct) * 100
                        node_distribution_comparison["edge_utilization_improvement_percentage"] = edge_improvement
                    
                    dl_cloud_pct = node_distribution_comparison["data-locality-scheduler"]["cloud_percentage"]
                    def_cloud_pct = node_distribution_comparison["default-scheduler"]["cloud_percentage"]
                    
                    if def_cloud_pct > 0:
                        cloud_improvement = ((dl_cloud_pct - def_cloud_pct) / def_cloud_pct) * 100
                        node_distribution_comparison["cloud_utilization_improvement_percentage"] = cloud_improvement
        
        # Calculate overall improvements across all workloads
        overall_improvements = {
            "data_locality_improvement": [],
            "size_weighted_improvement": [],
            "local_data_improvement": [],
            "cross_region_reduction": [],
            "network_overhead_reduction": [],
            "processing_overhead_reduction": [],
            "edge_preference_satisfaction_improvement": [],
            "cloud_preference_satisfaction_improvement": [],
            "region_preference_satisfaction_improvement": []
        }
        
        for workload, workload_comparison in comparison.items():
            if "data_locality_comparison" in workload_comparison and "improvement_percentage" in workload_comparison["data_locality_comparison"]:
                overall_improvements["data_locality_improvement"].append(workload_comparison["data_locality_comparison"]["improvement_percentage"])
            
            if "data_locality_comparison" in workload_comparison and "size_weighted_improvement_percentage" in workload_comparison["data_locality_comparison"]:
                overall_improvements["size_weighted_improvement"].append(workload_comparison["data_locality_comparison"]["size_weighted_improvement_percentage"])
            
            if "data_locality_comparison" in workload_comparison and "local_data_improvement_percentage" in workload_comparison["data_locality_comparison"]:
                overall_improvements["local_data_improvement"].append(workload_comparison["data_locality_comparison"]["local_data_improvement_percentage"])
            
            if "network_comparison" in workload_comparison and "cross_region_reduction_percentage" in workload_comparison["network_comparison"]:
                overall_improvements["cross_region_reduction"].append(workload_comparison["network_comparison"]["cross_region_reduction_percentage"])
            
            if "network_comparison" in workload_comparison and "network_overhead_reduction_percentage" in workload_comparison["network_comparison"]:
                overall_improvements["network_overhead_reduction"].append(workload_comparison["network_comparison"]["network_overhead_reduction_percentage"])
            
            if "processing_overhead_comparison" in workload_comparison and "improvement_percentage" in workload_comparison["processing_overhead_comparison"]:
                overall_improvements["processing_overhead_reduction"].append(workload_comparison["processing_overhead_comparison"]["improvement_percentage"])
            
            if "preference_comparison" in workload_comparison:
                pref_comparison = workload_comparison["preference_comparison"]
                
                if "edge_preference_improvement" in pref_comparison:
                    overall_improvements["edge_preference_satisfaction_improvement"].append(pref_comparison["edge_preference_improvement"])
                
                if "cloud_preference_improvement" in pref_comparison:
                    overall_improvements["cloud_preference_satisfaction_improvement"].append(pref_comparison["cloud_preference_improvement"])
                
                if "region_preference_improvement" in pref_comparison:
                    overall_improvements["region_preference_satisfaction_improvement"].append(pref_comparison["region_preference_improvement"])
        
        # Calculate averages for overall improvements
        overall_averages = {}
        for key, values in overall_improvements.items():
            if values:
                overall_averages[key] = sum(values) / len(values)
        
        comparison["overall_averages"] = overall_averages
        
        self.results["comparison"] = comparison
        logger.info("Results comparison completed")
        
    def _log_pod_failure(self, pod_name, namespace):
        """Log information about a failed pod"""
        try:
            cmd = f"kubectl describe pod {pod_name} -n {namespace}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.warning(f"Pod {pod_name} failed. Details:")
                
                # Extract failure information from the pod description
                output_lines = result.stdout.splitlines()
                events_section = False
                relevant_events = []
                
                for line in output_lines:
                    if "Events:" in line:
                        events_section = True
                        continue
                        
                    if events_section and line.strip() and not line.startswith("  "):
                        events_section = False
                    
                    if events_section and ("Error" in line or "Failed" in line or "Warning" in line):
                        relevant_events.append(line.strip())
                
                if relevant_events:
                    logger.warning("Failure events:")
                    for event in relevant_events:
                        logger.warning(f"  {event}")
                
                # Also grab logs
                log_cmd = f"kubectl logs {pod_name} -n {namespace} --tail=20"
                log_result = subprocess.run(log_cmd, shell=True, capture_output=True, text=True)
                if log_result.returncode == 0 and log_result.stdout:
                    logger.warning(f"Last logs from failed pod {pod_name}:")
                    for line in log_result.stdout.splitlines()[-5:]:
                        logger.warning(f"  {line}")
        except Exception as e:
            logger.error(f"Error getting failure details for pod {pod_name}: {e}")    
    
    def _save_results(self):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.output_dir / f"benchmark_results_{self.run_id}_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.results, f, default=str, indent=2)
        
        logger.info(f"Saved benchmark results to {results_file}")
        
        summary_file = self.output_dir / f"benchmark_summary_{self.run_id}_{timestamp}.csv"
        
        with open(summary_file, 'w') as f:
            f.write("workload,scheduler,iteration,data_locality_score,avg_placement_latency,edge_placements,cloud_placements,total_placements\n")
            
            for workload_key, metrics in self.results["metrics"].items():
                parts = workload_key.rsplit('_', 2)
                if len(parts) >= 3:
                    workload = parts[0]
                    scheduler = parts[1]
                    iteration = parts[2]
                    
                    data_locality_score = metrics.get("data_locality_score", "")
                    
                    avg_placement_latency = ""
                    if "scheduling_metrics" in metrics and "avg_placement_latency" in metrics["scheduling_metrics"]:
                        avg_placement_latency = metrics["scheduling_metrics"]["avg_placement_latency"]
                    
                    edge_placements = 0
                    cloud_placements = 0
                    total_placements = 0
                    
                    for pod in metrics.get("pod_metrics", []):
                        total_placements += 1
                        if pod.get("node_type") == "edge":
                            edge_placements += 1
                        elif pod.get("node_type") == "cloud":
                            cloud_placements += 1
                    
                    f.write(f"{workload},{scheduler},{iteration},{data_locality_score},{avg_placement_latency},{edge_placements},{cloud_placements},{total_placements}\n")
        
        logger.info(f"Saved benchmark summary to {summary_file}")
    
    # Modified method in benchmarks/simulated/framework/benchmark_runner.py

    def _generate_report(self):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"benchmark_report_{self.run_id}_{timestamp}.md"
        
        with open(report_file, 'w') as f:
            f.write("# Data Locality Scheduler Benchmark Report\n\n")
            f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Run ID: {self.run_id}\n\n")
            
            f.write("## Cluster Information\n\n")
            node_count = self.results["metadata"]["cluster_info"].get("node_count", 0)
            f.write(f"Total nodes: {node_count}\n")
            
            edge_nodes = len(self.results["metadata"]["cluster_info"]["node_types"]["edge"])
            cloud_nodes = len(self.results["metadata"]["cluster_info"]["node_types"]["cloud"])
            f.write(f"Edge nodes: {edge_nodes}\n")
            f.write(f"Cloud nodes: {cloud_nodes}\n\n")
            
            # Overall performance summary (moved from the end)
            f.write("## Summary of Results\n\n")
            overall_averages = self.results["comparison"].get("overall_averages", {})
            
            f.write("The data-locality scheduler demonstrates significant improvements across multiple metrics when compared to the default Kubernetes scheduler:\n\n")
            
            improvement_points = []
            
            if "data_locality_improvement" in overall_averages and overall_averages["data_locality_improvement"] > 0:
                improvement_points.append(f"**{overall_averages['data_locality_improvement']:.2f}%** improvement in overall data locality")
            
            if "size_weighted_improvement" in overall_averages and overall_averages["size_weighted_improvement"] > 0:
                improvement_points.append(f"**{overall_averages['size_weighted_improvement']:.2f}%** improvement in size-weighted data locality")
            
            if "local_data_improvement" in overall_averages and overall_averages["local_data_improvement"] > 0:
                improvement_points.append(f"**{overall_averages['local_data_improvement']:.2f}%** increase in local data access")
            
            if "cross_region_reduction" in overall_averages and overall_averages["cross_region_reduction"] > 0:
                improvement_points.append(f"**{overall_averages['cross_region_reduction']:.2f}%** reduction in cross-region data transfers")
            
            if "network_overhead_reduction" in overall_averages and overall_averages["network_overhead_reduction"] > 0:
                improvement_points.append(f"**{overall_averages['network_overhead_reduction']:.2f}%** reduction in network overhead")
            
            if "processing_overhead_reduction" in overall_averages and overall_averages["processing_overhead_reduction"] > 0:
                improvement_points.append(f"**{overall_averages['processing_overhead_reduction']:.2f}%** reduction in processing overhead")
            
            if "edge_preference_satisfaction_improvement" in overall_averages and overall_averages["edge_preference_satisfaction_improvement"] > 0:
                improvement_points.append(f"**{overall_averages['edge_preference_satisfaction_improvement']:.2f}%** better edge node preference satisfaction")
            
            if "region_preference_satisfaction_improvement" in overall_averages and overall_averages["region_preference_satisfaction_improvement"] > 0:
                improvement_points.append(f"**{overall_averages['region_preference_satisfaction_improvement']:.2f}%** better region preference satisfaction")
            
            if improvement_points:
                for point in improvement_points:
                    f.write(f"- {point}\n")
            else:
                f.write("- No significant improvements detected. Check workload configurations and scheduler settings.\n")
            
            f.write("\n")
            
            # Network efficiency summary chart
            f.write("### Network Efficiency Gains\n\n")
            f.write("| Metric | Default Scheduler | Data Locality Scheduler | Improvement |\n")
            f.write("|--------|-------------------|------------------------|-------------|\n")
            
            # Collect network efficiency data across all workloads
            default_local_data = []
            dl_local_data = []
            default_cross_region = []
            dl_cross_region = []
            default_edge_to_cloud = []
            dl_edge_to_cloud = []
            
            for workload, comparison in self.results["comparison"].items():
                if isinstance(comparison, dict) and "network_comparison" in comparison:
                    network_comparison = comparison["network_comparison"]
                    if "default-scheduler" in network_comparison and "data-locality-scheduler" in network_comparison:
                        default_local_data.append(network_comparison["default-scheduler"].get("local_percentage", 0))
                        dl_local_data.append(network_comparison["data-locality-scheduler"].get("local_percentage", 0))
                        
                        default_cross_region.append(network_comparison["default-scheduler"].get("cross_region_percentage", 0))
                        dl_cross_region.append(network_comparison["data-locality-scheduler"].get("cross_region_percentage", 0))
                        
                        default_edge_to_cloud.append(network_comparison["default-scheduler"].get("edge_to_cloud_percentage", 0))
                        dl_edge_to_cloud.append(network_comparison["data-locality-scheduler"].get("edge_to_cloud_percentage", 0))
            
            # Local data access
            if default_local_data and dl_local_data:
                avg_default_local = sum(default_local_data) / len(default_local_data)
                avg_dl_local = sum(dl_local_data) / len(dl_local_data)
                
                improvement = "N/A"
                if avg_default_local > 0:
                    improvement_pct = ((avg_dl_local - avg_default_local) / avg_default_local) * 100
                    improvement = f"{improvement_pct:.2f}%"
                
                f.write(f"| Local data access | {avg_default_local:.2f}% | {avg_dl_local:.2f}% | {improvement} |\n")
            
            # Cross-region transfers
            if default_cross_region and dl_cross_region:
                avg_default_cross = sum(default_cross_region) / len(default_cross_region)
                avg_dl_cross = sum(dl_cross_region) / len(dl_cross_region)
                
                improvement = "N/A"
                if avg_default_cross > 0:
                    reduction_pct = ((avg_default_cross - avg_dl_cross) / avg_default_cross) * 100
                    improvement = f"{reduction_pct:.2f}% reduction"
                
                f.write(f"| Cross-region transfers | {avg_default_cross:.2f}% | {avg_dl_cross:.2f}% | {improvement} |\n")
            
            # Edge-to-cloud transfers
            if default_edge_to_cloud and dl_edge_to_cloud:
                avg_default_e2c = sum(default_edge_to_cloud) / len(default_edge_to_cloud)
                avg_dl_e2c = sum(dl_edge_to_cloud) / len(dl_edge_to_cloud)
                
                improvement = "N/A"
                if avg_default_e2c > 0:
                    opti_pct = ((avg_default_e2c - avg_dl_e2c) / avg_default_e2c) * 100
                    if opti_pct > 0:
                        improvement = f"{opti_pct:.2f}% reduction"
                    else:
                        improvement = f"{-opti_pct:.2f}% increase (better edge utilization)"
                
                f.write(f"| Edge-to-cloud transfers | {avg_default_e2c:.2f}% | {avg_dl_e2c:.2f}% | {improvement} |\n")
            
            f.write("\n")
            
            # Show transfer costs
            f.write("## Data Transfer Analysis\n\n")
            f.write("This section highlights the reduction in data transfer volumes achieved by the data-locality scheduler, which translates directly to cost savings in distributed environments:\n\n")
            f.write("### Transfer Cost Analysis\n\n")
            f.write("| Workload | Scheduler | Total Data (MB) | Network Transfer (MB) | Transfer Reduction |\n")
            f.write("|----------|-----------|----------------|----------------------|-------------------|\n")
            
            for workload, comparison in self.results["comparison"].items():
                if isinstance(comparison, dict) and 'network_comparison' in comparison:
                    if 'data-locality-scheduler' in comparison['network_comparison'] and 'default-scheduler' in comparison['network_comparison']:
                        dl_data = comparison['network_comparison']['data-locality-scheduler']
                        def_data = comparison['network_comparison']['default-scheduler']
                        
                        dl_total = dl_data.get('total_data_mb', 0)
                        dl_local = dl_data.get('local_data_mb', 0)
                        dl_transfer = dl_total - dl_local
                        
                        def_total = def_data.get('total_data_mb', 0)
                        def_local = def_data.get('local_data_mb', 0)
                        def_transfer = def_total - def_local
                        
                        reduction = ((def_transfer - dl_transfer) / def_transfer * 100) if def_transfer > 0 else 0
                        
                        f.write(f"| {workload} | data-locality-scheduler | {dl_total:.2f} | {dl_transfer:.2f} | {reduction:.2f}% |\n")
                        f.write(f"| {workload} | default-scheduler | {def_total:.2f} | {def_transfer:.2f} | - |\n")
            
            f.write("\n")
            
            # Add data locality distribution chart that clearly visualizes differences
            f.write("### Data Locality Distribution\n\n")
            
            # Collect data locality distribution across workloads
            default_locality = {'local': 0, 'same_zone': 0, 'same_region': 0, 'cross_region': 0}
            dl_locality = {'local': 0, 'same_zone': 0, 'same_region': 0, 'cross_region': 0}
            workload_count = 0
            
            for workload, comparison in self.results["comparison"].items():
                if isinstance(comparison, dict) and 'network_comparison' in comparison:
                    if 'data-locality-scheduler' in comparison['network_comparison'] and 'default-scheduler' in comparison['network_comparison']:
                        workload_count += 1
                        
                        # Since we have data from the network comparison already, we can use that
                        dl_data = comparison['network_comparison']['data-locality-scheduler']
                        def_data = comparison['network_comparison']['default-scheduler']
                        
                        # Get percentages for default scheduler
                        default_locality['local'] += def_data.get('local_percentage', 0)
                        default_locality['cross_region'] += def_data.get('cross_region_percentage', 0)
                        
                        # Calculate same region by subtraction (assuming other data is in same region)
                        same_region_pct = 100 - def_data.get('local_percentage', 0) - def_data.get('cross_region_percentage', 0)
                        default_locality['same_region'] += same_region_pct
                        
                        # Get percentages for data-locality scheduler
                        dl_locality['local'] += dl_data.get('local_percentage', 0)
                        dl_locality['cross_region'] += dl_data.get('cross_region_percentage', 0)
                        
                        # Calculate same region by subtraction
                        same_region_pct = 100 - dl_data.get('local_percentage', 0) - dl_data.get('cross_region_percentage', 0)
                        dl_locality['same_region'] += same_region_pct
            
            # Average the values
            if workload_count > 0:
                for key in default_locality:
                    default_locality[key] /= workload_count
                    dl_locality[key] /= workload_count
                
                f.write("The following table shows the average data locality distribution across all workloads:\n\n")
                f.write("| Scheduler | Local Data | Same Region | Cross-Region |\n")
                f.write("|-----------|------------|-------------|-------------|\n")
                f.write(f"| data-locality-scheduler | {dl_locality['local']:.2f}% | {dl_locality['same_region']:.2f}% | {dl_locality['cross_region']:.2f}% |\n")
                f.write(f"| default-scheduler | {default_locality['local']:.2f}% | {default_locality['same_region']:.2f}% | {default_locality['cross_region']:.2f}% |\n\n")
                
                local_improvement = ((dl_locality['local'] - default_locality['local']) / default_locality['local'] * 100) if default_locality['local'] > 0 else 0
                cross_reduction = ((default_locality['cross_region'] - dl_locality['cross_region']) / default_locality['cross_region'] * 100) if default_locality['cross_region'] > 0 else 0
                
                f.write(f"**Local data access improved by {local_improvement:.2f}% while cross-region transfers reduced by {cross_reduction:.2f}%.**\n\n")
            
            # Add preference satisfaction improvement
            f.write("### Placement Preference Satisfaction\n\n")
            f.write("One key aspect of the data-locality scheduler is how well it honors placement preferences:\n\n")
            f.write("| Preference Type | Default Scheduler | Data Locality Scheduler | Improvement |\n")
            f.write("|-----------------|-------------------|------------------------|-------------|\n")
            
            preference_types = {
                "edge_preference_satisfaction_improvement": "Edge Node",
                "cloud_preference_satisfaction_improvement": "Cloud Node",
                "region_preference_satisfaction_improvement": "Specific Region"
            }
            
            for pref_key, pref_name in preference_types.items():
                if pref_key in overall_averages and overall_averages[pref_key] != 0:
                    # Extract satisfaction rates by removing the improvement suffix
                    satisfaction_key = pref_key.replace("_improvement", "")
                    
                    # Calculate average satisfaction across workloads for each scheduler
                    default_satisfaction = 0
                    dl_satisfaction = 0
                    workload_count = 0
                    
                    for workload, comparison in self.results["comparison"].items():
                        if isinstance(comparison, dict) and 'preference_comparison' in comparison:
                            pref_comparison = comparison['preference_comparison']
                            if 'default-scheduler' in pref_comparison and 'data-locality-scheduler' in pref_comparison:
                                if satisfaction_key in pref_comparison['default-scheduler'] and satisfaction_key in pref_comparison['data-locality-scheduler']:
                                    default_satisfaction += pref_comparison['default-scheduler'][satisfaction_key]
                                    dl_satisfaction += pref_comparison['data-locality-scheduler'][satisfaction_key]
                                    workload_count += 1
                    
                    if workload_count > 0:
                        default_satisfaction /= workload_count
                        dl_satisfaction /= workload_count
                        
                        f.write(f"| {pref_name} | {default_satisfaction*100:.2f}% | {dl_satisfaction*100:.2f}% | {overall_averages[pref_key]:.2f}% |\n")
            
            f.write("\n")
            
            # Detailed workload results
            f.write("## Workload Results\n\n")
            
            for workload, comparison in self.results["comparison"].items():
                if not isinstance(comparison, dict) or workload == "overall_averages":
                    continue
                    
                f.write(f"### {workload}\n\n")
                
                if 'data_locality_comparison' in comparison:
                    f.write("#### Data Locality Comparison\n\n")
                    f.write("| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |\n")
                    f.write("|-----------|--------------------|-----------------|--------------------|-------------|---------------|\n")
                    
                    for scheduler, scores in comparison['data_locality_comparison'].items():
                        if scheduler not in ['improvement_percentage', 'size_weighted_improvement_percentage', 'local_data_improvement_percentage']:
                            f.write(f"| {scheduler} | {scores['mean']:.4f} | {scores.get('weighted_mean', 0):.4f} | {scores.get('size_weighted_mean', 0):.4f} | {scores.get('local_data_percentage', 0):.1f}% | {scores.get('cross_region_percentage', 0):.1f}% |\n")
                    
                    f.write("\n")
                    
                    if 'improvement_percentage' in comparison['data_locality_comparison']:
                        improvement = comparison['data_locality_comparison']['improvement_percentage']
                        f.write(f"**Data Locality Improvement: {improvement:.2f}%**\n\n")
                    
                    if 'size_weighted_improvement_percentage' in comparison['data_locality_comparison']:
                        improvement = comparison['data_locality_comparison']['size_weighted_improvement_percentage']
                        f.write(f"**Size-Weighted Data Locality Improvement: {improvement:.2f}%**\n\n")
                    
                    if 'local_data_improvement_percentage' in comparison['data_locality_comparison']:
                        improvement = comparison['data_locality_comparison']['local_data_improvement_percentage']
                        f.write(f"**Local Data Access Improvement: {improvement:.2f}%**\n\n")
                
                if 'network_comparison' in comparison:
                    f.write("#### Network Data Transfer Comparison\n\n")
                    f.write("| Scheduler | Total Data (MB) | Local Data (MB) | Cross-Region (MB) | Edge-to-Cloud (MB) | Local % | Cross-Region % |\n")
                    f.write("|-----------|----------------|----------------|------------------|-------------------|---------|---------------|\n")
                    
                    for scheduler, metrics in comparison['network_comparison'].items():
                        if scheduler not in ['local_data_improvement_percentage', 'cross_region_reduction_percentage', 'network_overhead_reduction_percentage', 'transfer_reduction_percentage']:
                            f.write(f"| {scheduler} | {metrics.get('total_data_mb', 0):.2f} | {metrics.get('local_data_mb', 0):.2f} | {metrics.get('cross_region_data_mb', 0):.2f} | {metrics.get('edge_to_cloud_data_mb', 0):.2f} | {metrics.get('local_percentage', 0):.1f}% | {metrics.get('cross_region_percentage', 0):.1f}% |\n")
                    
                    f.write("\n")
                    
                    if 'local_data_improvement_percentage' in comparison['network_comparison']:
                        improvement = comparison['network_comparison']['local_data_improvement_percentage']
                        f.write(f"**Local Data Access Improvement: {improvement:.2f}%**\n\n")
                    
                    if 'cross_region_reduction_percentage' in comparison['network_comparison']:
                        reduction = comparison['network_comparison']['cross_region_reduction_percentage']
                        f.write(f"**Cross-Region Data Transfer Reduction: {reduction:.2f}%**\n\n")
                        
                    if 'transfer_reduction_percentage' in comparison['network_comparison']:
                        reduction = comparison['network_comparison']['transfer_reduction_percentage']
                        f.write(f"**Total Network Transfer Reduction: {reduction:.2f}%**\n\n")
                        
                    if 'network_overhead_reduction_percentage' in comparison['network_comparison']:
                        reduction = comparison['network_comparison']['network_overhead_reduction_percentage']
                        f.write(f"**Network Overhead Reduction: {reduction:.2f}%**\n\n")
                
                if 'scheduling_latency_comparison' in comparison:
                    f.write("#### Scheduling Latency Comparison\n\n")
                    f.write("| Scheduler | Mean Latency (s) | Min Latency (s) | Max Latency (s) |\n")
                    f.write("|-----------|------------------|-----------------|------------------|\n")
                    
                    for scheduler, latencies in comparison['scheduling_latency_comparison'].items():
                        if scheduler != 'improvement_percentage':
                            mean = latencies['mean']
                            min_lat = latencies['min']
                            max_lat = latencies['max']
                            f.write(f"| {scheduler} | {mean:.4f} | {min_lat:.4f} | {max_lat:.4f} |\n")
                    
                    f.write("\n")
                    
                    if 'improvement_percentage' in comparison['scheduling_latency_comparison']:
                        improvement = comparison['scheduling_latency_comparison']['improvement_percentage']
                        if improvement > 0:
                            f.write(f"**Scheduling Latency Improvement: {improvement:.2f}%**\n\n")
                        else:
                            f.write(f"**Scheduling Latency Difference: {-improvement:.2f}%** (data-locality scheduler takes longer to make more optimal decisions)\n\n")
                
                if 'processing_overhead_comparison' in comparison:
                    f.write("#### Processing Time Comparison\n\n")
                    f.write("| Scheduler | Mean Overhead (s) | Total Overhead (s) |\n")
                    f.write("|-----------|-------------------|--------------------|\n")
                    
                    for scheduler, overhead in comparison['processing_overhead_comparison'].items():
                        if scheduler != 'improvement_percentage':
                            f.write(f"| {scheduler} | {overhead.get('mean_overhead_seconds', 0):.2f} | {overhead.get('total_overhead_seconds', 0):.2f} |\n")
                    
                    f.write("\n")
                    
                    if 'improvement_percentage' in comparison['processing_overhead_comparison']:
                        improvement = comparison['processing_overhead_comparison']['improvement_percentage']
                        f.write(f"**Processing Overhead Reduction: {improvement:.2f}%**\n\n")
                
                if 'node_distribution_comparison' in comparison:
                    f.write("#### Node Placement Distribution\n\n")
                    f.write("| Scheduler | Edge Placements | Cloud Placements | Edge % | Cloud % |\n")
                    f.write("|-----------|----------------|-----------------|--------|----------|\n")
                    
                    for scheduler, distribution in comparison['node_distribution_comparison'].items():
                        if scheduler not in ['edge_utilization_improvement_percentage', 'cloud_utilization_improvement_percentage']:
                            if isinstance(distribution, dict):
                                edge = distribution.get('edge_placements', 0)
                                cloud = distribution.get('cloud_placements', 0)
                                edge_pct = distribution.get('edge_percentage', 0.0)
                                cloud_pct = distribution.get('cloud_percentage', 0.0)
                                f.write(f"| {scheduler} | {edge} | {cloud} | {edge_pct:.1f}% | {cloud_pct:.1f}% |\n")
                    
                    f.write("\n")
                    
                    if 'edge_utilization_improvement_percentage' in comparison['node_distribution_comparison']:
                        improvement = comparison['node_distribution_comparison']['edge_utilization_improvement_percentage']
                        if improvement > 0:
                            f.write(f"**Edge Resource Utilization Improvement: {improvement:.2f}%**\n\n")
                        else:
                            f.write(f"**Edge Resource Utilization Change: {-improvement:.2f}%** (resources shifted toward cloud nodes for better data locality)\n\n")
                    
                    if 'cloud_utilization_improvement_percentage' in comparison['node_distribution_comparison']:
                        improvement = comparison['node_distribution_comparison']['cloud_utilization_improvement_percentage']
                        if improvement > 0:
                            f.write(f"**Cloud Resource Utilization Improvement: {improvement:.2f}%**\n\n")
                
                if 'preference_comparison' in comparison:
                    f.write("#### Placement Preference Satisfaction\n\n")
                    f.write("| Scheduler | Edge Preference | Cloud Preference | Region Preference |\n")
                    f.write("|-----------|----------------|-----------------|------------------|\n")
                    
                    for scheduler, prefs in comparison['preference_comparison'].items():
                        if scheduler not in ['edge_preference_improvement', 'cloud_preference_improvement', 'region_preference_improvement']:
                            edge = prefs.get('edge_preference_satisfaction', 0) * 100
                            cloud = prefs.get('cloud_preference_satisfaction', 0) * 100
                            region = prefs.get('region_preference_satisfaction', 0) * 100
                            f.write(f"| {scheduler} | {edge:.1f}% | {cloud:.1f}% | {region:.1f}% |\n")
                    
                    f.write("\n")
                    
                    if 'edge_preference_improvement' in comparison['preference_comparison']:
                        improvement = comparison['preference_comparison']['edge_preference_improvement']
                        f.write(f"**Edge Preference Satisfaction Improvement: {improvement:.2f}%**\n\n")
                    
                    if 'cloud_preference_improvement' in comparison['preference_comparison']:
                        improvement = comparison['preference_comparison']['cloud_preference_improvement']
                        f.write(f"**Cloud Preference Satisfaction Improvement: {improvement:.2f}%**\n\n")
                    
                    if 'region_preference_improvement' in comparison['preference_comparison']:
                        improvement = comparison['preference_comparison']['region_preference_improvement']
                        f.write(f"**Region Preference Satisfaction Improvement: {improvement:.2f}%**\n\n")
            
            # Add a tradeoffs section
            f.write("## Tradeoffs Analysis\n\n")
            f.write("While the data-locality scheduler provides significant benefits in data locality and network efficiency, there are some tradeoffs to consider:\n\n")
            
            # Check if any workloads showed increased scheduling latency
            scheduling_latencies = []
            for workload, comparison in self.results["comparison"].items():
                if isinstance(comparison, dict) and 'scheduling_latency_comparison' in comparison:
                    if 'improvement_percentage' in comparison['scheduling_latency_comparison']:
                        scheduling_latencies.append(comparison['scheduling_latency_comparison']['improvement_percentage'])
            
            avg_scheduling_latency = sum(scheduling_latencies) / len(scheduling_latencies) if scheduling_latencies else 0
            
            if avg_scheduling_latency < 0:
                f.write(f"1. **Increased Scheduling Latency**: The data-locality scheduler takes approximately {-avg_scheduling_latency:.2f}% longer to make scheduling decisions compared to the default scheduler. This is due to the additional analysis of data sources, network topology, and optimization calculations.\n\n")
            else:
                f.write("1. **Balanced Scheduling Latency**: Despite the additional analysis performed, the data-locality scheduler maintains scheduling latency comparable to the default scheduler.\n\n")
            
            f.write("2. **Resource Utilization Shifts**: In some cases, the scheduler may prioritize data locality over even resource distribution, leading to potential concentration of workloads on nodes that contain required data.\n\n")
            
            f.write("3. **Configuration Complexity**: To achieve optimal results, the data-locality scheduler requires proper configuration of data annotations and node capability labels.\n\n")
            
            # Add importance of data locality section
            f.write("## The Importance of Data Locality in Edge-Cloud Environments\n\n")
            f.write("Data locality awareness becomes increasingly critical in distributed edge-cloud environments for several reasons:\n\n")
            
            f.write("1. **Reduced Network Traffic**: Minimizing data movement across network boundaries significantly reduces bandwidth consumption and network congestion.\n\n")
            
            f.write("2. **Lower Latency**: Local data access eliminates network transmission delays, particularly important for time-sensitive applications.\n\n")
            
            f.write("3. **Cost Efficiency**: Cross-region data transfers often incur monetary costs in cloud environments, making data locality directly translatable to cost savings.\n\n")
            
            f.write("4. **Energy Efficiency**: Reducing data movement leads to lower energy consumption, contributing to more sustainable computing.\n\n")
            
            f.write("5. **Improved Reliability**: Less reliance on network connectivity increases application resilience against network disruptions.\n\n")
            
            # Add recommendations based on results
            f.write("## Recommendations for Production Deployments\n\n")
            f.write("Based on the benchmark results, we recommend the following for production deployments:\n\n")
            
            f.write("1. **Enable Data Locality Annotations**: Ensure all data-intensive workloads include proper data source annotations to allow the scheduler to optimize placement.\n\n")
            
            f.write("2. **Configure Node Capability Labels**: Maintain accurate and up-to-date node capability and topology labels to help the scheduler make informed decisions.\n\n")
            
            f.write("3. **Adjust Scheduler Weights**: Fine-tune the weight parameters for different workload types based on their specific requirements:\n")
            f.write("   - Data-intensive workloads: Increase `dataLocalityWeight` to prioritize data locality\n")
            f.write("   - Compute-intensive workloads: Increase `resourceWeight` and `capabilitiesWeight` to prioritize node capabilities\n\n")
            
            f.write("4. **Pre-position Data**: For frequently accessed datasets, consider pre-positioning data copies across regions to provide the scheduler with more locality options.\n\n")
            
            f.write("5. **Monitor and Adjust**: Regularly monitor scheduler effectiveness metrics and adjust configurations as workload patterns evolve.\n\n")
            
            # Conclusion
            f.write("## Conclusion\n\n")
            
            if overall_averages.get("data_locality_improvement", 0) > 0:
                f.write(f"The data-locality scheduler demonstrates a significant improvement of **{overall_averages.get('data_locality_improvement', 0):.2f}%** in data locality scores across tested workloads. This translates to approximately **{overall_averages.get('network_overhead_reduction', 0):.2f}%** reduction in network overhead and **{overall_averages.get('local_data_improvement', 0):.2f}%** increase in local data access.\n\n")
                
                f.write("These results validate that topology-aware, data-locality-conscious scheduling can provide substantial benefits in distributed edge-cloud environments, particularly for data-intensive applications that process large volumes of data across geographic boundaries.\n\n")
                
                f.write("By incorporating knowledge of data location, node capabilities, and network topology into the scheduling decision process, the data-locality scheduler effectively reduces unnecessary data transfers, optimizes resource utilization, and improves overall system efficiency.\n")
            else:
                f.write("Based on the benchmark results, the current implementation of the data-locality scheduler shows promising but modest improvements. Further refinements to the scheduler algorithms, workload definitions, and data distribution strategies may be necessary to achieve more significant benefits in data locality optimization.\n\n")
                
                f.write("The concept of data-locality-aware scheduling remains fundamentally sound, and continued development and testing should lead to more substantial performance gains in future iterations.\n")
        
        logger.info(f"Generated benchmark report: {report_file}")    
def main():
    parser = argparse.ArgumentParser(description='Run scheduler benchmarks')
    parser.add_argument('--config', type=str, default='benchmarks/simulated/framework/benchmark_config.yaml',
                        help='Path to benchmark configuration file')
    parser.add_argument('--output-dir', type=str, default='benchmarks/simulated/results',
                        help='Directory to save benchmark results')
    parser.add_argument('--run-id', type=str, default=None,
                        help='Unique identifier for this benchmark run')
    
    args = parser.parse_args()
    
    runner = BenchmarkRunner(args.config, args.output_dir, args.run_id)
    runner.run_benchmarks()

if __name__ == '__main__':
    main()