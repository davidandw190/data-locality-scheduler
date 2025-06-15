import logging
from analyzer import Analyzer

logger = logging.getLogger("metrics-collector")

class MetricsCollector:
    
    def __init__(self, cluster_manager, storage_manager):
        self.cluster_manager = cluster_manager
        self.storage_manager = storage_manager
        self.data_locality_analyzer = Analyzer(cluster_manager, storage_manager)
    
    def collect_workload_metrics(self, workload_name, scheduler_name, iteration, run_id):
        namespace = self.cluster_manager.k8s_client.api_client.configuration.host
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        
        logger.info(f"Collecting metrics for workload {workload_key}")
        
        pods = self.cluster_manager.get_pods_in_namespace(
            "scheduler-benchmark",
            f"benchmark-run-id={run_id},iteration={iteration}"
        )
        
        metrics = {
            "pod_metrics": [],
            "scheduling_metrics": {
                "placement_latencies": [],
                "scheduling_attempts": 0,
                "scheduling_errors": 0
            },
            "data_locality_metrics": {}
        }
        
        pod_metrics = self._collect_pod_metrics(pods.items)
        metrics["pod_metrics"] = pod_metrics
        
        self._collect_scheduling_metrics(pods.items, metrics["scheduling_metrics"])
        
        locality_metrics = self.data_locality_analyzer.calculate_data_locality_metrics(
            pod_metrics, workload_name
        )
        metrics["data_locality_metrics"] = locality_metrics
        
        return metrics
    
    def _collect_pod_metrics(self, pods):
        pod_metrics = []
        
        for pod in pods:
            placement = {
                "pod_name": pod.metadata.name,
                "node": pod.spec.node_name,
                "phase": pod.status.phase,
                "start_time": pod.status.start_time.timestamp() if pod.status.start_time else None,
                "data_annotations": {
                    k: v for k, v in pod.metadata.annotations.items() 
                    if k.startswith('data.scheduler.thesis/')
                },
                "placement_latency": self._calculate_placement_latency(pod),
                "container_statuses": self._extract_container_statuses(pod)
            }
            
            self._add_node_information(placement, pod.spec.node_name)
            pod_metrics.append(placement)
        
        return pod_metrics
    
    def _calculate_placement_latency(self, pod):
        if not pod.status.conditions:
            return None
        
        for condition in pod.status.conditions:
            if condition.type == 'PodScheduled' and condition.status == 'True':
                if condition.last_transition_time and pod.metadata.creation_timestamp:
                    scheduling_time = condition.last_transition_time.timestamp()
                    creation_time = pod.metadata.creation_timestamp.timestamp()
                    return scheduling_time - creation_time
        
        return None
    
    def _extract_container_statuses(self, pod):
        if not pod.status.container_statuses:
            return []
        
        container_statuses = []
        for container in pod.status.container_statuses:
            status = {
                "name": container.name,
                "ready": container.ready,
                "restart_count": container.restart_count,
                "state": self._determine_container_state(container.state)
            }
            container_statuses.append(status)
        
        return container_statuses
    
    def _determine_container_state(self, state):
        if state.running:
            return "running"
        elif state.terminated:
            return "terminated"
        elif state.waiting:
            return "waiting"
        return "unknown"
    
    def _add_node_information(self, placement, node_name):
        if not node_name:
            return
        
        try:
            node = self.cluster_manager.read_node(node_name)
            placement["node_type"] = node.metadata.labels.get("node-capability/node-type", "unknown")
            placement["node_region"] = node.metadata.labels.get("topology.kubernetes.io/region", "")
            placement["node_zone"] = node.metadata.labels.get("topology.kubernetes.io/zone", "")
            
            placement["node_capabilities"] = {
                k.replace('node-capability/', ''): v 
                for k, v in node.metadata.labels.items() 
                if k.startswith('node-capability/')
            }
        except Exception as e:
            logger.warning(f"Failed to get node information for {node_name}: {e}")
    
    def _collect_scheduling_metrics(self, pods, scheduling_metrics):
        pod_names = [pod.metadata.name for pod in pods]
        
        scheduling_attempts = 0
        scheduling_errors = 0
        scheduling_events = []
        
        for pod in pods:
            events = self.cluster_manager.get_events_for_pod("scheduler-benchmark", pod.metadata.name)
            
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
        
        scheduling_metrics["scheduling_attempts"] = scheduling_attempts
        scheduling_metrics["scheduling_errors"] = scheduling_errors
        scheduling_metrics["scheduling_events"] = scheduling_events
        
        placement_latencies = [
            pod_metric.get("placement_latency") 
            for pod_metric in scheduling_metrics.get("placement_latencies", [])
            if pod_metric.get("placement_latency") is not None
        ]
        
        if placement_latencies:
            scheduling_metrics["avg_placement_latency"] = sum(placement_latencies) / len(placement_latencies)
            scheduling_metrics["min_placement_latency"] = min(placement_latencies)
            scheduling_metrics["max_placement_latency"] = max(placement_latencies)