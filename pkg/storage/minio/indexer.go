package minio

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"

	"slices"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// discovers and indexes Minio storage nodes and their contents
type Indexer struct {
	storageIndex     *storage.StorageIndex
	serviceClients   map[string]*Client // service name -> client
	serviceToNodeMap map[string]string  // service name -> node name
	clients          map[string]*Client // node name -> client
	mu               sync.RWMutex
	refreshInterval  time.Duration
}

func NewIndexer(storageIndex *storage.StorageIndex, refreshInterval time.Duration) *Indexer {
	return &Indexer{
		storageIndex:     storageIndex,
		serviceClients:   make(map[string]*Client),
		serviceToNodeMap: make(map[string]string),
		clients:          make(map[string]*Client),
		refreshInterval:  refreshInterval,
	}
}

func (i *Indexer) RegisterMinioNode(nodeName, endpoint string, secure bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	client := NewClient(endpoint, secure)
	i.clients[nodeName] = client
	klog.V(2).Infof("Registered Minio client for node %s at %s", nodeName, endpoint)
}

func (i *Indexer) RegisterMinioService(serviceName, endpoint string, secure bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	client := NewClient(endpoint, secure)
	i.serviceClients[serviceName] = client
	klog.V(2).Infof("Registered MinIO client for service %s at %s", serviceName, endpoint)
}

func (i *Indexer) AssociateServiceWithNode(serviceName, nodeName string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.serviceToNodeMap[serviceName] = nodeName
	klog.V(2).Infof("Associated MinIO service %s with node %s", serviceName, nodeName)
}

func (i *Indexer) GetNodeForService(serviceName string) string {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.serviceToNodeMap[serviceName]
}

func (i *Indexer) RegisterBucketForNode(bucket string, nodeName string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	bucketNodes := i.storageIndex.GetBucketNodes(bucket)
	if bucketNodes == nil {
		bucketNodes = []string{nodeName}
	} else if !containsString(bucketNodes, nodeName) {
		bucketNodes = append(bucketNodes, nodeName)
	}

	i.storageIndex.RegisterBucket(bucket, bucketNodes)
	klog.V(3).Infof("Associated bucket %s with node %s", bucket, nodeName)
}

func (i *Indexer) RefreshIndex(ctx context.Context) error {
	klog.V(2).Info("Starting MinIO index refresh")

	i.mu.RLock()
	clients := make(map[string]*Client)
	for k, v := range i.clients {
		clients[k] = v
	}
	i.mu.RUnlock()

	i.mu.RLock()
	for k, v := range i.serviceClients {
		clients[k] = v
	}
	i.mu.RUnlock()

	refreshCount := 0
	for nodeName, client := range clients {
		if err := i.refreshNodeIndex(ctx, nodeName, client); err != nil {
			klog.Warningf("Failed to refresh index for %s: %v", nodeName, err)
		} else {
			refreshCount++
		}
	}

	klog.V(2).Infof("MinIO index refresh complete: processed %d out of %d clients",
		refreshCount, len(clients))
	return nil
}

func (i *Indexer) refreshNodeIndex(ctx context.Context, nodeName string, client *Client) error {
	klog.V(3).Infof("Refreshing index for %s", nodeName)

	if err := client.Connect(); err != nil {
		if _, exists := i.serviceClients[nodeName]; exists {
			klog.V(3).Infof("Reconnecting to service %s", nodeName)
			client = NewClient(client.endpoint, client.secure)
			if err := client.Connect(); err != nil {
				return fmt.Errorf("failed to connect to Minio service %s: %w", nodeName, err)
			}
			i.mu.Lock()
			i.serviceClients[nodeName] = client
			i.mu.Unlock()
		} else {
			return fmt.Errorf("failed to connect to Minio on node %s: %w", nodeName, err)
		}
	}

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets on %s: %w", nodeName, err)
	}

	actualNodeName := nodeName
	isService := false

	if strings.Contains(nodeName, "minio") && !strings.Contains(nodeName, "os-k8s") {
		isService = true

		if mappedNode := i.GetNodeForService(nodeName); mappedNode != "" {
			klog.V(3).Infof("Found service %s mapping to node %s in cache", nodeName, mappedNode)
			actualNodeName = mappedNode
		} else {
			podNodeName, err := i.resolveServiceToNodeName(ctx, nodeName)
			if err == nil && podNodeName != "" {
				klog.V(2).Infof("Resolved service %s to node %s", nodeName, podNodeName)
				actualNodeName = podNodeName
				i.AssociateServiceWithNode(nodeName, podNodeName)
			} else {
				klog.Warningf("Could not resolve service %s to node: %v, skipping", nodeName, err)
				return fmt.Errorf("unable to resolve service %s to a node name, skipping", nodeName)
			}
		}
	}

	storageNode, exists := i.storageIndex.GetStorageNode(actualNodeName)
	if !exists {
		var nodeType storage.StorageNodeType = storage.StorageTypeCloud
		var region, zone string

		k8sNode, err := getNodeByName(ctx, actualNodeName)
		if err == nil {
			if nodeTypeLabel, ok := k8sNode.Labels["node-capability/node-type"]; ok && nodeTypeLabel == "edge" {
				nodeType = storage.StorageTypeEdge
			}

			region = k8sNode.Labels["topology.kubernetes.io/region"]
			zone = k8sNode.Labels["topology.kubernetes.io/zone"]
		} else {
			klog.Warningf("Could not get node details for %s: %v", actualNodeName, err)

			if isService {
				if strings.Contains(strings.ToLower(nodeName), "edge") {
					nodeType = storage.StorageTypeEdge
				}

				// Try to infer region/zone from service name as fallback
				if strings.Contains(nodeName, "region1") || strings.Contains(nodeName, "region-1") {
					region = "region-1"
					if nodeType == storage.StorageTypeEdge {
						zone = "zone-1-edge"
					} else {
						zone = "zone-1-cloud"
					}
				} else if strings.Contains(nodeName, "region2") || strings.Contains(nodeName, "region-2") {
					region = "region-2"
					zone = "zone-2-edge"
				} else {
					// Default for main MinIO
					region = "region-1"
					zone = "zone-1-cloud"
				}
			}
		}

		storageNode = &storage.StorageNode{
			Name:              actualNodeName,
			NodeType:          nodeType,
			ServiceType:       storage.StorageServiceMinio,
			Region:            region,
			Zone:              zone,
			CapacityBytes:     10 * 1024 * 1024 * 1024, // default 10GB
			AvailableBytes:    8 * 1024 * 1024 * 1024,  // default 8GB available
			StorageTechnology: "minio",
			LastUpdated:       time.Now(),
			Buckets:           make([]string, 0, len(buckets)),
			TopologyLabels:    make(map[string]string),
		}
	} else {
		// Update existing node
		storageNode.LastUpdated = time.Now()
		storageNode.ServiceType = storage.StorageServiceMinio
	}

	// Update node buckets - but don't erase existing buckets that might have been
	// added from other detection methods
	existingBuckets := make(map[string]bool)
	for _, bucket := range storageNode.Buckets {
		existingBuckets[bucket] = true
	}

	for _, bucket := range buckets {
		if !existingBuckets[bucket] {
			storageNode.Buckets = append(storageNode.Buckets, bucket)
		}
	}

	i.storageIndex.RegisterOrUpdateStorageNode(storageNode)

	objectCount := 0
	for _, bucket := range buckets {
		objectCount += i.registerBucketAndObjects(ctx, client, bucket, actualNodeName)
	}

	klog.V(2).Infof("Refreshed index for %s (actual node: %s): found %d buckets and %d objects",
		nodeName, actualNodeName, len(buckets), objectCount)
	return nil
}

func (i *Indexer) resolveServiceToNodeName(ctx context.Context, serviceName string) (string, error) {
	if strings.HasPrefix(serviceName, "os-k8s-") {
		return serviceName, nil
	}

	knownServices := map[string]string{
		"minio":              "os-k8s-worker-node-0",
		"minio-edge-region1": "os-k8s-worker-node-1",
		"minio-edge-region2": "os-k8s-worker-node-2",
	}

	serviceNameOnly := strings.Split(serviceName, ":")[0]

	if nodeName, exists := knownServices[serviceNameOnly]; exists {
		klog.V(3).Infof("Resolved service %s to node %s via hardcoded mapping",
			serviceName, nodeName)
		return nodeName, nil
	}

	clientset, err := getK8sClientset()
	if err != nil {
		return "", err
	}

	targetNamespace := "data-locality-scheduler"

	labelSelectors := []string{
		fmt.Sprintf("app=%s", serviceNameOnly),
		"app=minio",
		"app=minio-edge",
	}

	if strings.Contains(serviceNameOnly, "region") {
		region := ""
		if strings.Contains(serviceNameOnly, "region1") {
			region = "region-1"
		} else if strings.Contains(serviceNameOnly, "region2") {
			region = "region-2"
		}

		if region != "" {
			labelSelectors = append(labelSelectors,
				fmt.Sprintf("app=minio-edge,region=%s", region))
		}
	}

	for _, selector := range labelSelectors {
		pods, err := clientset.CoreV1().Pods(targetNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})

		if err == nil && len(pods.Items) > 0 {
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" && pod.Status.Phase == v1.PodRunning {
					klog.V(2).Infof("Resolved service %s to node %s via selector %s in namespace %s",
						serviceName, pod.Spec.NodeName, selector, targetNamespace)
					return pod.Spec.NodeName, nil
				}
			}
		}
	}

	// if not found in target namespace, try all namespaces
	for _, selector := range labelSelectors {
		pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})

		if err == nil && len(pods.Items) > 0 {
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" && pod.Status.Phase == v1.PodRunning {
					klog.V(2).Infof("Resolved service %s to node %s via selector %s across all namespaces",
						serviceName, pod.Spec.NodeName, selector)
					return pod.Spec.NodeName, nil
				}
			}
		}
	}

	return "", fmt.Errorf("could not resolve service %s to any node", serviceName)
}

func getK8sClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func getNodeByName(ctx context.Context, nodeName string) (*v1.Node, error) {
	clientset, err := getK8sClientset()
	if err != nil {
		return nil, err
	}

	return clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
}

func (i *Indexer) registerBucketAndObjects(ctx context.Context, client *Client, bucket, nodeName string) int {
	klog.V(3).Infof("Registering bucket %s for node %s", bucket, nodeName)

	i.RegisterBucketForNode(bucket, nodeName)

	objects, err := client.ListObjects(ctx, bucket)
	if err != nil {
		klog.Warningf("Failed to list objects in bucket %s on node %s: %v",
			bucket, nodeName, err)
		return 0
	}

	objectCount := 0
	for _, obj := range objects {
		objectCount++
		urn := fmt.Sprintf("%s/%s", bucket, obj.Key)

		existingItem, exists := i.storageIndex.GetDataItem(urn)
		var locations []string

		if exists {
			locations = make([]string, len(existingItem.Locations))
			copy(locations, existingItem.Locations)

			if !containsString(locations, nodeName) {
				locations = append(locations, nodeName)
			}
		} else {
			locations = []string{nodeName}
		}

		metadata := make(map[string]string)
		metadata["content-type"] = obj.ContentType

		i.storageIndex.AddDataItem(&storage.DataItem{
			URN:          urn,
			Size:         obj.Size,
			Locations:    locations,
			LastModified: obj.LastModified,
			ContentType:  obj.ContentType,
			Metadata:     metadata,
		})
	}

	klog.V(3).Infof("Indexed %d objects in bucket %s on node %s",
		objectCount, bucket, nodeName)
	return objectCount
}

func (i *Indexer) StartRefresher(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(i.refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := i.RefreshIndex(ctx); err != nil {
					klog.Warningf("Error refreshing storage index: %v", err)
				}
			case <-ctx.Done():
				klog.Info("Stopping storage index refresher")
				return
			}
		}
	}()
}

func (i *Indexer) DiscoverMinioNodesFromKubernetes(ctx context.Context, clientset kubernetes.Interface) error {
	namespace := "data-locality-scheduler"
	discoveredCount := 0

	// we try multiple selectors to find all MinIO pods
	selectors := []string{
		"app=minio",
		"app=minio-edge",
		"app in (minio, minio-edge)",
	}

	for _, selector := range selectors {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})

		if err != nil {
			klog.Warningf("Failed to list MinIO pods with selector %s: %v", selector, err)
			continue
		}

		for _, pod := range pods.Items {
			if pod.Spec.NodeName == "" || pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
				continue
			}

			nodeName := pod.Spec.NodeName
			podIP := pod.Status.PodIP
			port := 9000

			for _, container := range pod.Spec.Containers {
				for _, containerPort := range container.Ports {
					if containerPort.ContainerPort == 9000 {
						port = int(containerPort.ContainerPort)
						break
					}
				}
			}

			endpoint := fmt.Sprintf("%s:%d", podIP, port)
			i.RegisterMinioNode(nodeName, endpoint, false)

			serviceName := pod.Labels["app"]
			if region, hasRegion := pod.Labels["region"]; hasRegion {
				serviceName = fmt.Sprintf("%s-%s", serviceName, region)
			}

			i.RegisterMinioService(serviceName, endpoint, false)
			i.AssociateServiceWithNode(serviceName, nodeName)
			i.AssociateServiceWithNode(pod.Name, nodeName)

			discoveredCount++
			klog.V(2).Infof("Discovered MinIO pod %s on node %s (IP: %s)",
				pod.Name, nodeName, podIP)
		}
	}

	knownMappings := map[string]struct {
		service  string
		node     string
		endpoint string
	}{
		"central": {
			service:  "minio",
			node:     "os-k8s-worker-node-0",
			endpoint: "minio.data-locality-scheduler.svc.cluster.local:9000",
		},
		"edge1": {
			service:  "minio-edge-region1",
			node:     "os-k8s-worker-node-1",
			endpoint: "minio-edge-region1.data-locality-scheduler.svc.cluster.local:9000",
		},
		"edge2": {
			service:  "minio-edge-region2",
			node:     "os-k8s-worker-node-2",
			endpoint: "minio-edge-region2.data-locality-scheduler.svc.cluster.local:9000",
		},
	}

	for key, mapping := range knownMappings {
		i.RegisterMinioNode(mapping.node, mapping.endpoint, false)
		i.RegisterMinioService(mapping.service, mapping.endpoint, false)
		i.AssociateServiceWithNode(mapping.service, mapping.node)

		klog.V(2).Infof("Added explicit mapping for %s: service %s on node %s",
			key, mapping.service, mapping.node)
		discoveredCount++
	}

	klog.V(2).Infof("Discovered %d MinIO instances in the cluster", discoveredCount)
	return nil
}

func (i *Indexer) DiscoverMinioServicesFromKubernetes(ctx context.Context, clientset kubernetes.Interface) error {
	namespace := "data-locality-scheduler"

	services, err := clientset.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app in (minio, minio-edge)",
	})

	if err != nil {
		return fmt.Errorf("failed to list MinIO services: %w", err)
	}

	if len(services.Items) == 0 {
		services, err = clientset.CoreV1().Services(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list all services: %w", err)
		}

		var filteredItems []v1.Service
		for _, svc := range services.Items {
			if strings.Contains(strings.ToLower(svc.Name), "minio") {
				filteredItems = append(filteredItems, svc)
			}
		}
		services.Items = filteredItems
	}

	serviceNodeMap := make(map[string]string)

	for _, svc := range services.Items {
		var selector strings.Builder
		for k, v := range svc.Spec.Selector {
			if selector.Len() > 0 {
				selector.WriteString(",")
			}
			selector.WriteString(fmt.Sprintf("%s=%s", k, v))
		}

		if selector.Len() == 0 && svc.Labels["app"] != "" {
			// fallback: try using the service's app label
			selector.WriteString(fmt.Sprintf("app=%s", svc.Labels["app"]))
		}

		if selector.Len() > 0 {
			pods, err := clientset.CoreV1().Pods(svc.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: selector.String(),
			})

			if err == nil && len(pods.Items) > 0 {
				for _, pod := range pods.Items {
					if pod.Status.Phase == v1.PodRunning && pod.Spec.NodeName != "" {
						serviceNodeMap[svc.Name] = pod.Spec.NodeName
						break
					}
				}
			}
		}
	}

	discoveredCount := 0
	for _, svc := range services.Items {
		serviceDNS := fmt.Sprintf("%s.%s.svc.cluster.local:%d",
			svc.Name, svc.Namespace, 9000)

		if svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != "None" {
			port := 9000
			for _, servicePort := range svc.Spec.Ports {
				if servicePort.Port == 9000 || strings.Contains(servicePort.Name, "api") {
					port = int(servicePort.Port)
					break
				}
			}

			clusterIPEndpoint := fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, port)
			i.RegisterMinioService(svc.Name+"-ip", clusterIPEndpoint, false)
		}

		i.RegisterMinioService(svc.Name, serviceDNS, false)
		discoveredCount++

		i.RegisterMinioService(fmt.Sprintf("%s-%s", svc.Namespace, svc.Name),
			serviceDNS, false)

		if nodeName, exists := serviceNodeMap[svc.Name]; exists {
			klog.V(2).Infof("Service %s runs on node %s", svc.Name, nodeName)
			i.AssociateServiceWithNode(svc.Name, nodeName)

			i.RegisterMinioNode(nodeName, serviceDNS, false)
		}
	}

	defaultServices := []string{
		"minio.data-locality-scheduler.svc.cluster.local:9000",
		"minio-edge-region1.data-locality-scheduler.svc.cluster.local:9000",
		"minio-edge-region2.data-locality-scheduler.svc.cluster.local:9000",
	}

	defaultNodeMap := map[string]string{
		"minio":              "os-k8s-worker-node-0",
		"minio-edge-region1": "os-k8s-worker-node-1",
		"minio-edge-region2": "os-k8s-worker-node-2",
	}

	for _, svc := range defaultServices {
		parts := strings.Split(svc, ".")
		if len(parts) > 0 {
			serviceName := parts[0]
			i.RegisterMinioService(serviceName, svc, false)

			if nodeName, exists := defaultNodeMap[serviceName]; exists {
				i.AssociateServiceWithNode(serviceName, nodeName)
				i.RegisterMinioNode(nodeName, svc, false)
			}

			discoveredCount++
		}
	}

	klog.V(2).Infof("Discovered %d MinIO services in the cluster", discoveredCount)
	return nil
}

func containsString(slice []string, s string) bool {
	return slices.Contains(slice, s)
}
