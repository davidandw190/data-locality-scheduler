import logging
import subprocess
import time
from pathlib import Path

logger = logging.getLogger("storage-manager")

class StorageManager:
    
    def __init__(self, config_manager, cluster_manager):
        self.config_manager = config_manager
        self.cluster_manager = cluster_manager
        self.namespace = config_manager.get_namespace()
    
    def deploy_storage_services(self):
        logger.info("Deploying storage services")
        
        storage_manifests = Path("benchmarks/simulated/kubernetes/storage-config.yaml")
        if not storage_manifests.exists():
            raise FileNotFoundError(f"Storage manifest not found: {storage_manifests}")
        
        cmd = ["kubectl", "apply", "-f", str(storage_manifests)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to deploy storage: {result.stderr}")
        
        logger.info("Successfully deployed storage services")
    
    def wait_for_readiness(self):
        logger.info("Waiting for storage services to be ready")
        
        timeout = 300
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            pods = self.cluster_manager.get_pods_in_namespace(
                self.namespace, "app=minio"
            )
            
            ready_count = 0
            for pod in pods.items:
                if self._is_pod_ready(pod):
                    ready_count += 1
            
            if ready_count >= 3:
                logger.info(f"Storage services are ready: {ready_count} pods running")
                return True
            
            logger.info(f"Waiting for storage services: {ready_count}/{len(pods.items)} pods ready")
            time.sleep(5)
        
        logger.warning("Storage services not fully ready within timeout")
        return False
    
    def _is_pod_ready(self, pod):
        if pod.status.phase != 'Running':
            return False
        
        if not pod.status.container_statuses:
            return False
        
        return all(container.ready for container in pod.status.container_statuses)
    
    def initialize_data(self):
        logger.info("Initializing benchmark data")
        
        cmd = [
            "python", 
            "benchmarks/simulated/framework/data_initializer.py",
            "--config", str(self.config_manager.config_file),
            "--workloads-dir", "benchmarks/simulated/workloads"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to initialize benchmark data: {result.stderr}")
            logger.warning("Proceeding with benchmarks, but data locality measurements may be inaccurate")
        else:
            logger.info("Successfully initialized benchmark data")
    
    def get_storage_node_mapping(self):
        storage_nodes = {}
        bucket_to_nodes = {}
        
        pods = self.cluster_manager.get_pods_in_namespace(self.namespace, "app=minio")
        
        for pod in pods.items:
            if pod.status.phase != 'Running' or not pod.spec.node_name:
                continue
            
            node_name = pod.spec.node_name
            labels = pod.metadata.labels
            role = labels.get('role', 'unknown')
            region = labels.get('region', 'central')
            
            service_name = self._determine_service_name(pod.metadata.name, role, region)
            if service_name:
                storage_nodes[service_name] = node_name
                self._map_buckets_for_service(service_name, node_name, bucket_to_nodes)
        
        return storage_nodes, bucket_to_nodes
    
    def _determine_service_name(self, pod_name, role, region):
        if role == 'central':
            return "minio-central"
        elif role == 'edge' and region == 'region-1':
            return "minio-edge-region1"
        elif role == 'edge' and region == 'region-2':
            return "minio-edge-region2"
        return None
    
    def _map_buckets_for_service(self, service_name, node_name, bucket_to_nodes):
        bucket_mapping = {
            "minio-central": ["datasets", "intermediate", "results", "shared", "test-bucket"],
            "minio-edge-region1": ["edge-data", "region1-bucket"],
            "minio-edge-region2": ["region2-bucket"]
        }
        
        buckets = bucket_mapping.get(service_name, [])
        for bucket in buckets:
            if bucket not in bucket_to_nodes:
                bucket_to_nodes[bucket] = []
            if node_name not in bucket_to_nodes[bucket]:
                bucket_to_nodes[bucket].append(node_name)