package minio

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// discovers and indexes Minio storage nodes and their contents
type Indexer struct {
	storageIndex    *storage.StorageIndex
	serviceClients  map[string]*Client // service name -> client
	clients         map[string]*Client // node name -> client
	mu              sync.RWMutex
	refreshInterval time.Duration
}

func NewIndexer(storageIndex *storage.StorageIndex, refreshInterval time.Duration) *Indexer {
	return &Indexer{
		storageIndex:    storageIndex,
		serviceClients:  make(map[string]*Client),
		clients:         make(map[string]*Client),
		refreshInterval: refreshInterval,
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
		podNodeName, err := i.resolveServiceToNodeName(ctx, nodeName)
		if err == nil && podNodeName != "" {
			klog.V(3).Infof("Resolved service %s to node %s", nodeName, podNodeName)
			actualNodeName = podNodeName
		} else {
			klog.V(3).Infof("Could not resolve service %s to node: %v", nodeName, err)
		}
	}

	storageNode, exists := i.storageIndex.GetStorageNode(actualNodeName)
	if !exists {
		var nodeType storage.StorageNodeType = storage.StorageTypeCloud
		var region, zone string

		if isService {
			if strings.Contains(strings.ToLower(nodeName), "edge") {
				nodeType = storage.StorageTypeEdge
			}

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
				// default for main MinIO
				region = "region-1"
				zone = "zone-1-cloud"
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
		// we update existing node
		storageNode.LastUpdated = time.Now()
		storageNode.ServiceType = storage.StorageServiceMinio
	}

	// update node buckets
	storageNode.Buckets = make([]string, len(buckets))
	copy(storageNode.Buckets, buckets)

	i.storageIndex.RegisterOrUpdateStorageNode(storageNode)

	// register buckets and objects
	objectCount := 0
	for _, bucket := range buckets {
		objectCount += i.registerBucketAndObjects(ctx, client, bucket, actualNodeName)
	}

	klog.V(3).Infof("Refreshed index for %s: found %d buckets and %d objects",
		nodeName, len(buckets), objectCount)
	return nil
}

func (i *Indexer) resolveServiceToNodeName(ctx context.Context, serviceName string) (string, error) {
	// if we already have a pod->node mapping, use it
	if strings.HasPrefix(serviceName, "os-k8s-") {
		return serviceName, nil // already a node name
	}

	// try to find pods running this service
	clientset, err := getK8sClientset()
	if err != nil {
		return "", err
	}

	// extract just the service name without port
	serviceNameOnly := strings.Split(serviceName, ":")[0]

	labelSelectors := []string{
		fmt.Sprintf("app=%s", serviceNameOnly),
		"app in (minio, minio-edge)",
		fmt.Sprintf("app in (minio-%s, %s-edge)",
			strings.TrimPrefix(serviceNameOnly, "minio-"),
			serviceNameOnly),
	}

	for _, selector := range labelSelectors {
		pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})

		if err == nil && len(pods.Items) > 0 {
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" && pod.Status.Phase == v1.PodRunning {
					return pod.Spec.NodeName, nil
				}
			}
		}
	}

	return serviceName, fmt.Errorf("could not find pods for service %s", serviceName)
}

func getK8sClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func (i *Indexer) registerBucketAndObjects(ctx context.Context, client *Client, bucket, nodeName string) int {
	bucketNodes := i.storageIndex.GetBucketNodes(bucket)
	if bucketNodes == nil {
		bucketNodes = []string{nodeName}
	} else if !containsString(bucketNodes, nodeName) {
		bucketNodes = append(bucketNodes, nodeName)
	}
	i.storageIndex.RegisterBucket(bucket, bucketNodes)

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
			locations = existingItem.Locations
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

	klog.V(4).Infof("Indexed %d objects in bucket %s on node %s",
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
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: "app in (minio, minio-edge)",
	})
	if err != nil {
		return fmt.Errorf("failed to list MinIO pods: %w", err)
	}

	discoveredCount := 0
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" || pod.Status.Phase != v1.PodRunning {
			continue
		}

		nodeName := pod.Spec.NodeName
		podIP := pod.Status.PodIP
		if podIP == "" {
			klog.V(3).Infof("Skipping MinIO pod %s/%s: no pod IP", pod.Namespace, pod.Name)
			continue
		}

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
		discoveredCount++

		i.RegisterMinioService(pod.Name, endpoint, false)
	}

	klog.V(2).Infof("Discovered %d MinIO instances in the cluster", discoveredCount)
	return nil
}

func (i *Indexer) DiscoverMinioServicesFromKubernetes(ctx context.Context, clientset kubernetes.Interface) error {
	services, err := clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{
		LabelSelector: "app in (minio, minio-edge)",
	})

	if err != nil {
		return fmt.Errorf("failed to list MinIO services: %w", err)
	}

	if len(services.Items) == 0 {
		services, err = clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{})
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

		klog.V(2).Infof("Discovered MinIO service: %s at %s", svc.Name, serviceDNS)
		i.RegisterMinioService(svc.Name, serviceDNS, false)
		discoveredCount++

		i.RegisterMinioService(fmt.Sprintf("%s-%s", svc.Namespace, svc.Name),
			serviceDNS, false)
	}

	defaultServices := []string{
		"minio.data-locality-scheduler.svc.cluster.local:9000",
		"minio-edge-region1.data-locality-scheduler.svc.cluster.local:9000",
		"minio-edge-region2.data-locality-scheduler.svc.cluster.local:9000",
	}

	for _, svc := range defaultServices {
		parts := strings.Split(svc, ".")
		if len(parts) > 0 {
			i.RegisterMinioService(parts[0], svc, false)
			discoveredCount++
		}
	}

	klog.V(2).Infof("Discovered %d MinIO services in the cluster", discoveredCount)
	return nil
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
