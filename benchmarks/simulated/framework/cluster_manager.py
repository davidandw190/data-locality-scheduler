import logging
import kubernetes.client
import kubernetes.config
from kubernetes.client.rest import ApiException

logger = logging.getLogger("cluster-manager")

class ClusterManager:
    
    def __init__(self):
        self._initialize_kubernetes_client()
    
    def _initialize_kubernetes_client(self):
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
    
    def ensure_namespace(self, namespace):
        try:
            self.k8s_client.read_namespace(name=namespace)
            logger.info(f"Using existing namespace: {namespace}")
        except ApiException as e:
            if e.status == 404:
                ns_body = kubernetes.client.V1Namespace(
                    metadata=kubernetes.client.V1ObjectMeta(name=namespace)
                )
                self.k8s_client.create_namespace(body=ns_body)
                logger.info(f"Created namespace: {namespace}")
            else:
                logger.error(f"Error checking namespace: {e}")
                raise
    
    def collect_cluster_info(self, cluster_info_dict):
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
                    {"type": condition.type, "status": condition.status}
                    for condition in node.status.conditions
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
        
        cluster_info_dict["nodes"] = node_info
        cluster_info_dict["node_count"] = len(node_info)
        
        self._categorize_nodes(cluster_info_dict, node_info)
        logger.info(f"Collected information for {len(node_info)} nodes")
    
    def _categorize_nodes(self, cluster_info_dict, node_info):
        cluster_info_dict["node_types"] = {"edge": [], "cloud": [], "other": []}
        
        for node in node_info:
            node_type = node["labels"].get("node-capability/node-type", "other")
            if node_type == "edge":
                cluster_info_dict["node_types"]["edge"].append(node["name"])
            elif node_type == "cloud":
                cluster_info_dict["node_types"]["cloud"].append(node["name"])
            else:
                cluster_info_dict["node_types"]["other"].append(node["name"])
    
    def get_pods_in_namespace(self, namespace, label_selector=None):
        return self.k8s_client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector
        )
    
    def get_events_for_pod(self, namespace, pod_name):
        field_selector = f"involvedObject.name={pod_name}"
        return self.k8s_client.list_namespaced_event(
            namespace=namespace,
            field_selector=field_selector
        )
    
    def read_node(self, node_name):
        return self.k8s_client.read_node(name=node_name)
    
    def read_pod(self, namespace, pod_name):
        return self.k8s_client.read_namespaced_pod(name=pod_name, namespace=namespace)