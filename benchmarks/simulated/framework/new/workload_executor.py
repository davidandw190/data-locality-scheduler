import logging
import subprocess
import time
import yaml
from pathlib import Path

logger = logging.getLogger("workload-executor")

class WorkloadExecutor:
    
    def __init__(self, config_manager, cluster_manager):
        self.config_manager = config_manager
        self.cluster_manager = cluster_manager
        self.namespace = config_manager.get_namespace()
        self.execution_results = {}
    
    def execute_workload(self, workload_name, scheduler_name, iteration, run_id):
        workload_file = Path(f"benchmarks/simulated/workloads/{workload_name}.yaml")
        if not workload_file.exists():
            logger.error(f"Workload file not found: {workload_file}")
            return False
        
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        run_suffix = f"{run_id}-{iteration}"
        
        try:
            modified_workload = self._prepare_workload(
                workload_file, scheduler_name, run_id, iteration, run_suffix
            )
            
            temp_file = self._save_modified_workload(
                modified_workload, workload_name, scheduler_name, iteration
            )
            
            start_time = time.time()
            success = self._deploy_workload(temp_file)
            
            if success:
                self.execution_results[workload_key] = {
                    "start_time": start_time,
                    "deploy_duration": time.time() - start_time,
                    "status": "deployed",
                    "iteration": iteration
                }
                
                self._wait_for_completion(workload_name, scheduler_name, iteration, run_id)
                self._cleanup_workload(temp_file)
            
            return success
            
        except Exception as e:
            logger.error(f"Error executing workload: {e}")
            return False
    
    def _prepare_workload(self, workload_file, scheduler_name, run_id, iteration, run_suffix):
        with open(workload_file, 'r') as f:
            workload_yaml = f.read()
        
        workload = list(yaml.safe_load_all(workload_yaml))
        modified_workload = []
        
        for item in workload:
            if item.get('kind') == 'Pod':
                self._modify_pod_spec(item, scheduler_name, run_id, iteration, run_suffix)
            modified_workload.append(item)
        
        return modified_workload
    
    def _modify_pod_spec(self, pod_spec, scheduler_name, run_id, iteration, run_suffix):
        original_name = pod_spec['metadata']['name']
        pod_spec['metadata']['name'] = f"{original_name}-{run_suffix}"
        pod_spec['spec']['schedulerName'] = scheduler_name
        
        self._add_annotations(pod_spec, run_id, scheduler_name, iteration)
        self._add_labels(pod_spec, run_id, scheduler_name, iteration)
        self._add_environment_variables(pod_spec, run_id, scheduler_name, iteration)
        self._add_tolerations(pod_spec)
    
    def _add_annotations(self, pod_spec, run_id, scheduler_name, iteration):
        if 'annotations' not in pod_spec['metadata']:
            pod_spec['metadata']['annotations'] = {}
        
        pod_spec['metadata']['annotations'].update({
            'benchmark.thesis/run-id': run_id,
            'benchmark.thesis/scheduler': scheduler_name,
            'benchmark.thesis/iteration': str(iteration)
        })
    
    def _add_labels(self, pod_spec, run_id, scheduler_name, iteration):
        if 'labels' not in pod_spec['metadata']:
            pod_spec['metadata']['labels'] = {}
        
        pod_spec['metadata']['labels'].update({
            'benchmark-run-id': run_id,
            'scheduler': scheduler_name.replace('-', ''),
            'iteration': str(iteration)
        })
    
    def _add_environment_variables(self, pod_spec, run_id, scheduler_name, iteration):
        env_vars = [
            {'name': 'BENCHMARK_RUN_ID', 'value': run_id},
            {'name': 'SCHEDULER_NAME', 'value': scheduler_name},
            {'name': 'ITERATION', 'value': str(iteration)},
            {'name': 'WORKLOAD_DURATION', 'value': '60'}
        ]
        
        for container in pod_spec['spec'].get('containers', []):
            if 'env' not in container:
                container['env'] = []
            container['env'].extend(env_vars)
            
            self._add_data_environment_variables(container, pod_spec['metadata'])
    
    def _add_data_environment_variables(self, container, metadata):
        for k, v in metadata.get('annotations', {}).items():
            if k.startswith('data.scheduler.thesis/'):
                env_name = k.replace('data.scheduler.thesis/', '').replace('-', '_').upper()
                container['env'].append({
                    'name': f"DATA_SCHEDULER_THESIS_{env_name}",
                    'value': v
                })
    
    def _add_tolerations(self, pod_spec):
        if 'tolerations' not in pod_spec['spec']:
            pod_spec['spec']['tolerations'] = []
        
        tolerations = [
            {
                'key': 'node-role.kubernetes.io/control-plane',
                'operator': 'Exists',
                'effect': 'NoSchedule'
            },
            {
                'key': 'node-role.kubernetes.io/master',
                'operator': 'Exists',
                'effect': 'NoSchedule'
            }
        ]
        
        pod_spec['spec']['tolerations'].extend(tolerations)
    
    def _save_modified_workload(self, modified_workload, workload_name, scheduler_name, iteration):
        temp_file = Path(f"benchmarks/simulated/results/tmp_{workload_name}_{scheduler_name}_{iteration}.yaml")
        with open(temp_file, 'w') as f:
            yaml.dump_all(modified_workload, f)
        return temp_file
    
    def _deploy_workload(self, temp_file):
        cmd = ["kubectl", "apply", "-f", str(temp_file)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to deploy workload: {result.stderr}")
            return False
        
        logger.info("Workload deployed successfully")
        return True
    
    def _wait_for_completion(self, workload_name, scheduler_name, iteration, run_id):
        max_wait_time = self.config_manager.get_execution_config().get('max_wait_time', 300)
        poll_interval = self.config_manager.get_execution_config().get('poll_interval', 5)
        
        start_time = time.time()
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        
        while time.time() - start_time < max_wait_time:
            pods = self.cluster_manager.get_pods_in_namespace(
                self.namespace,
                f"benchmark-run-id={run_id},iteration={iteration}"
            )
            
            completed, failed, still_running = self._analyze_pod_status(pods.items)
            
            total_pods = len(pods.items)
            if total_pods > 0 and (completed + failed) == total_pods:
                status = "completed" if failed == 0 else "failed"
                self.execution_results[workload_key]["status"] = status
                self.execution_results[workload_key]["completion_time"] = time.time()
                self.execution_results[workload_key]["duration"] = time.time() - start_time
                return
            
            logger.info(f"Workload {workload_key}: {still_running} running, {completed} completed, {failed} failed")
            time.sleep(poll_interval)
        
        logger.warning(f"Workload {workload_key} did not complete within {max_wait_time} seconds")
        self.execution_results[workload_key]["status"] = "timeout"
        self.execution_results[workload_key]["completion_time"] = time.time()
        self.execution_results[workload_key]["duration"] = max_wait_time
    
    def _analyze_pod_status(self, pods):
        completed = sum(1 for pod in pods if pod.status.phase == 'Succeeded')
        failed = sum(1 for pod in pods if pod.status.phase == 'Failed')
        still_running = sum(1 for pod in pods if pod.status.phase in ['Pending', 'Running'])
        return completed, failed, still_running
    
    def _cleanup_workload(self, temp_file):
        if self.config_manager.get_execution_config().get('cleanup_after_run', True):
            cmd = ["kubectl", "delete", "-f", str(temp_file)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.warning(f"Failed to clean up workload: {result.stderr}")
            else:
                logger.info("Workload cleaned up successfully")
    
    def get_execution_results(self, workload_key):
        return self.execution_results.get(workload_key, {})