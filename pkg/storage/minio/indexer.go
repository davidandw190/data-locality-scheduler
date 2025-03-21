// pkg/storage/minio/indexer.go
package minio

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Indexer discovers and indexes Minio storage nodes and their contents
type Indexer struct {
	storageIndex    *storage.StorageIndex
	clients         map[string]*Client // node name -> client
	mu              sync.RWMutex
	refreshInterval time.Duration
}

func NewIndexer(storageIndex *storage.StorageIndex, refreshInterval time.Duration) *Indexer {
	return &Indexer{
		storageIndex:    storageIndex,
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

func (i *Indexer) RefreshIndex(ctx context.Context) error {
	i.mu.RLock()
	clients := make(map[string]*Client)
	for k, v := range i.clients {
		clients[k] = v
	}
	i.mu.RUnlock()

	for nodeName, client := range clients {
		if err := i.refreshNodeIndex(ctx, nodeName, client); err != nil {
			klog.Warningf("Failed to refresh index for node %s: %v", nodeName, err)
		}
	}

	return nil
}

func (i *Indexer) refreshNodeIndex(ctx context.Context, nodeName string, client *Client) error {
	if err := client.Connect(); err != nil {
		return fmt.Errorf("failed to connect to Minio on node %s: %w", nodeName, err)
	}

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets on node %s: %w", nodeName, err)
	}

	storageNode, exists := i.storageIndex.GetStorageNode(nodeName)
	if !exists {
		// Create new storage node entry
		storageNode = &storage.StorageNode{
			Name:        nodeName,
			NodeType:    storage.StorageTypeCloud, // Default
			ServiceType: storage.StorageServiceMinio,
			Buckets:     buckets,
			LastUpdated: time.Now(),
		}
	} else {
		// Update existing node
		storageNode.Buckets = buckets
		storageNode.ServiceType = storage.StorageServiceMinio
		storageNode.LastUpdated = time.Now()
	}

	// Register updated node
	i.storageIndex.RegisterOrUpdateStorageNode(storageNode)

	// Register buckets and objects
	for _, bucket := range buckets {
		// Register bucket
		bucketNodes := i.storageIndex.GetBucketNodes(bucket)
		if bucketNodes == nil {
			bucketNodes = []string{nodeName}
		} else if !containsString(bucketNodes, nodeName) {
			bucketNodes = append(bucketNodes, nodeName)
		}
		i.storageIndex.RegisterBucket(bucket, bucketNodes)

		// List and register objects
		objects, err := client.ListObjects(ctx, bucket)
		if err != nil {
			klog.Warningf("Failed to list objects in bucket %s on node %s: %v", bucket, nodeName, err)
			continue
		}

		// Register each object
		for _, obj := range objects {
			dataItem := &storage.DataItem{
				URN:          fmt.Sprintf("%s/%s", bucket, obj.Key),
				Size:         obj.Size,
				Locations:    []string{nodeName},
				LastModified: obj.LastModified,
				ContentType:  obj.ContentType,
			}
			i.storageIndex.AddDataItem(dataItem)
		}
	}

	klog.V(3).Infof("Refreshed index for node %s: found %d buckets", nodeName, len(buckets))
	return nil
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
	// Find Minio pods
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: "app=minio",
	})
	if err != nil {
		return fmt.Errorf("failed to list Minio pods: %w", err)
	}

	discoveredCount := 0
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" || pod.Status.Phase != v1.PodRunning {
			continue
		}

		nodeName := pod.Spec.NodeName
		podIP := pod.Status.PodIP
		if podIP == "" {
			klog.V(3).Infof("Skipping Minio pod %s/%s: no pod IP", pod.Namespace, pod.Name)
			continue
		}

		// Find the API port
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

		// Register the Minio node with default credentials
		i.RegisterMinioNode(nodeName, endpoint, false)
		discoveredCount++
	}

	klog.V(2).Infof("Discovered %d Minio instances in the cluster", discoveredCount)
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
