import logging

logger = logging.getLogger("data-locality-analyzer")

class Analyzer:
    
    def __init__(self, cluster_manager, storage_manager):
        self.cluster_manager = cluster_manager
        self.storage_manager = storage_manager
    
    def calculate_data_locality_metrics(self, pod_metrics, workload_name):
        logger.info(f"Calculating data locality metrics for {workload_name}")
        
        storage_nodes, bucket_to_nodes = self.storage_manager.get_storage_node_mapping()
        
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
        
        pod_node_topology = self._build_pod_topology_map(pod_metrics)
        
        for pod_metric in pod_metrics:
            pod_locality = self._analyze_pod_data_locality(
                pod_metric, pod_node_topology, bucket_to_nodes
            )
            
            if pod_locality:
                total_data_refs += pod_locality["total_refs"]
                local_data_refs += pod_locality["local_refs"]
                same_zone_refs += pod_locality["same_zone_refs"]
                same_region_refs += pod_locality["same_region_refs"]
                cross_region_refs += pod_locality["cross_region_refs"]
                
                total_data_size += pod_locality["total_data_size"]
                local_data_size += pod_locality["local_data_size"]
                same_zone_data_size += pod_locality["same_zone_data_size"]
                same_region_data_size += pod_locality["same_region_data_size"]
                cross_region_data_size += pod_locality["cross_region_data_size"]
                
                edge_to_cloud_transfers += pod_locality.get("edge_to_cloud_transfers", 0)
                cloud_to_edge_transfers += pod_locality.get("cloud_to_edge_transfers", 0)
                edge_to_cloud_data_size += pod_locality.get("edge_to_cloud_data_size", 0)
                cloud_to_edge_data_size += pod_locality.get("cloud_to_edge_data_size", 0)
        
        return self._calculate_locality_scores(
            total_data_refs, local_data_refs, same_zone_refs, same_region_refs, cross_region_refs,
            total_data_size, local_data_size, same_zone_data_size, same_region_data_size, cross_region_data_size,
            edge_to_cloud_transfers, cloud_to_edge_transfers, edge_to_cloud_data_size, cloud_to_edge_data_size
        )
    
    def _build_pod_topology_map(self, pod_metrics):
        pod_topology = {}
        
        for pod_metric in pod_metrics:
            pod_name = pod_metric.get('pod_name')
            pod_node = pod_metric.get('node')
            
            if not pod_node or not pod_name:
                continue
            
            try:
                node = self.cluster_manager.read_node(pod_node)
                pod_topology[pod_name] = {
                    'node': pod_node,
                    'region': node.metadata.labels.get('topology.kubernetes.io/region', ''),
                    'zone': node.metadata.labels.get('topology.kubernetes.io/zone', ''),
                    'node_type': node.metadata.labels.get('node-capability/node-type', 'unknown')
                }
            except Exception as e:
                logger.warning(f"Failed to get topology for pod {pod_name} on node {pod_node}: {e}")
        
        return pod_topology
    
    def _analyze_pod_data_locality(self, pod_metric, pod_topology, bucket_to_nodes):
        pod_name = pod_metric.get('pod_name')
        data_refs = pod_metric.get('data_annotations', {})
        
        if not data_refs or pod_name not in pod_topology:
            return None
        
        pod_info = pod_topology[pod_name]
        
        locality_counters = {
            "total_refs": 0,
            "local_refs": 0,
            "same_zone_refs": 0,
            "same_region_refs": 0,
            "cross_region_refs": 0,
            "total_data_size": 0,
            "local_data_size": 0,
            "same_zone_data_size": 0,
            "same_region_data_size": 0,
            "cross_region_data_size": 0,
            "edge_to_cloud_transfers": 0,
            "cloud_to_edge_transfers": 0,
            "edge_to_cloud_data_size": 0,
            "cloud_to_edge_data_size": 0
        }
        
        for key, value in data_refs.items():
            if key.startswith('data.scheduler.thesis/'):
                self._process_data_reference(
                    value, pod_info, bucket_to_nodes, locality_counters
                )
        
        if locality_counters["total_refs"] > 0:
            locality_score = locality_counters["local_refs"] / locality_counters["total_refs"]
            pod_metric['data_locality'] = {
                'score': locality_score,
                **locality_counters
            }
        
        return locality_counters
    
    def _process_data_reference(self, data_ref_value, pod_info, bucket_to_nodes, counters):
        parts = data_ref_value.split(',')
        if len(parts) < 2:
            return
        
        data_path = parts[0]
        try:
            data_size = int(parts[1])
        except (ValueError, IndexError):
            data_size = 0
        
        bucket = data_path.split('/')[0] if '/' in data_path else data_path
        
        counters["total_refs"] += 1
        counters["total_data_size"] += data_size
        
        locality_type = self._determine_data_locality(
            bucket, pod_info, bucket_to_nodes
        )
        
        self._update_locality_counters(locality_type, data_size, pod_info, counters)
    
    def _determine_data_locality(self, bucket, pod_info, bucket_to_nodes):
        pod_node = pod_info['node']
        pod_region = pod_info['region']
        pod_zone = pod_info['zone']
        
        if bucket not in bucket_to_nodes:
            return "UNKNOWN"
        
        bucket_nodes = bucket_to_nodes[bucket]
        
        if pod_node in bucket_nodes:
            return "LOCAL"
        
        for node_name in bucket_nodes:
            try:
                node = self.cluster_manager.read_node(node_name)
                node_region = node.metadata.labels.get('topology.kubernetes.io/region', '')
                node_zone = node.metadata.labels.get('topology.kubernetes.io/zone', '')
                
                if pod_zone and node_zone and pod_zone == node_zone:
                    return "SAME_ZONE"
                elif pod_region and node_region and pod_region == node_region:
                    return "SAME_REGION"
            except Exception as e:
                logger.warning(f"Error checking node {node_name} topology: {e}")
        
        return "CROSS_REGION"
    
    def _update_locality_counters(self, locality_type, data_size, pod_info, counters):
        if locality_type == "LOCAL":
            counters["local_refs"] += 1
            counters["local_data_size"] += data_size
        elif locality_type == "SAME_ZONE":
            counters["same_zone_refs"] += 1
            counters["same_zone_data_size"] += data_size
        elif locality_type == "SAME_REGION":
            counters["same_region_refs"] += 1
            counters["same_region_data_size"] += data_size
        else:
            counters["cross_region_refs"] += 1
            counters["cross_region_data_size"] += data_size
    
    def _calculate_locality_scores(self, total_refs, local_refs, same_zone_refs, same_region_refs, cross_region_refs,
                                 total_size, local_size, same_zone_size, same_region_size, cross_region_size,
                                 edge_cloud_transfers, cloud_edge_transfers, edge_cloud_size, cloud_edge_size):
        
        if total_refs == 0:
            return self._empty_locality_metrics()
        
        overall_score = local_refs / total_refs
        
        weighted_score = (
            local_refs * 1.0 + 
            same_zone_refs * 0.8 + 
            same_region_refs * 0.5
        ) / total_refs
        
        size_weighted_score = 0
        if total_size > 0:
            size_weighted_score = (
                (local_size * 1.0) + 
                (same_zone_size * 0.8) + 
                (same_region_size * 0.5)
            ) / total_size
        
        return {
            "overall_score": overall_score,
            "weighted_score": weighted_score,
            "size_weighted_score": size_weighted_score,
            "local_refs": local_refs,
            "same_zone_refs": same_zone_refs,
            "same_region_refs": same_region_refs,
            "cross_region_refs": cross_region_refs,
            "total_refs": total_refs,
            "local_data_size": local_size,
            "same_zone_data_size": same_zone_size,
            "same_region_data_size": same_region_size,
            "cross_region_data_size": cross_region_size,
            "total_data_size": total_size,
            "network_metrics": {
                "total_data_size_bytes": total_size,
                "local_data_size_bytes": local_size,
                "same_zone_data_size_bytes": same_zone_size,
                "same_region_data_size_bytes": same_region_size,
                "cross_region_data_size_bytes": cross_region_size,
                "edge_to_cloud_transfers": edge_cloud_transfers,
                "cloud_to_edge_transfers": cloud_edge_transfers,
                "edge_to_cloud_data_size_bytes": edge_cloud_size,
                "cloud_to_edge_data_size_bytes": cloud_edge_size
            }
        }
    
    def _empty_locality_metrics(self):
        return {
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