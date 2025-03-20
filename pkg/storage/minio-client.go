package storage

import (
	"context"
	"fmt"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// MinioClient integrates with MinIO instances for data discovery
type MinioClient struct {
	clientset    kubernetes.Interface
	storageIndex *StorageIndex
	minioClients map[string]*minio.Client
	namespace    string
}

func NewMinioClient(clientset kubernetes.Interface, storageIndex *StorageIndex, namespace string) *MinioClient {
	return &MinioClient{
		clientset:    clientset,
		storageIndex: storageIndex,
		minioClients: make(map[string]*minio.Client),
		namespace:    namespace,
	}
}

func (m *MinioClient) DiscoverMinioInstances(ctx context.Context) error {
	klog.Info("Discovering MinIO instances...")

	services, err := m.clientset.CoreV1().Services(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=minio",
	})
	if err != nil {
		return fmt.Errorf("failed to list MinIO services: %w", err)
	}

	klog.Infof("Found %d MinIO services", len(services.Items))

	for _, service := range services.Items {
		endpoints, err := m.clientset.CoreV1().Endpoints(m.namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if err != nil {
			klog.Warningf("Failed to get endpoints for MinIO service %s: %v", service.Name, err)
			continue
		}

		for _, subset := range endpoints.Subsets {
			for _, addr := range subset.Addresses {
				nodeName := ""
				if addr.NodeName != nil {
					nodeName = *addr.NodeName
				}

				if nodeName == "" {
					if addr.TargetRef != nil && addr.TargetRef.Kind == "Pod" {
						pod, err := m.clientset.CoreV1().Pods(m.namespace).Get(ctx, addr.TargetRef.Name, metav1.GetOptions{})
						if err == nil {
							nodeName = pod.Spec.NodeName
						}
					}
				}

				if nodeName == "" {
					klog.Warningf("Could not determine node name for MinIO endpoint %s", addr.IP)
					continue
				}

				endpoint := fmt.Sprintf("%s:%d", addr.IP, service.Spec.Ports[0].Port)
				if err := m.connectToMinIO(ctx, endpoint, nodeName); err != nil {
					klog.Warningf("Failed to connect to MinIO at %s on node %s: %v", endpoint, nodeName, err)
					continue
				}
			}
		}
	}

	return nil
}

func (m *MinioClient) connectToMinIO(ctx context.Context, endpoint string, nodeName string) error {
	// TODO: for now, we are using default credentials
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	useSSL := false

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	m.minioClients[nodeName] = client
	klog.Infof("Successfully connected to MinIO instance on node %s at %s", nodeName, endpoint)
	return nil
}

func (m *MinioClient) IndexAllBuckets(ctx context.Context) error {
	for nodeName, client := range m.minioClients {
		klog.Infof("Indexing buckets on node %s", nodeName)

		buckets, err := client.ListBuckets(ctx)
		if err != nil {
			klog.Warningf("Failed to list buckets on node %s: %v", nodeName, err)
			continue
		}

		var nodeBuckets []string
		for _, bucket := range buckets {
			bucketName := bucket.Name
			nodeBuckets = append(nodeBuckets, bucketName)

			// Get current nodes for this bucket
			currentNodes := m.storageIndex.GetBucketNodes(bucketName)
			if !containsString(currentNodes, nodeName) {
				newNodes := append(currentNodes, nodeName)
				m.storageIndex.RegisterBucket(bucketName, newNodes)
				klog.V(4).Infof("Registered bucket %s on node %s", bucketName, nodeName)
			}

			// Index objects in this bucket
			if err := m.indexBucketObjects(ctx, client, bucketName, nodeName); err != nil {
				klog.Warningf("Failed to index objects in bucket %s on node %s: %v", bucketName, nodeName, err)
			}
		}

		// we update storage node with buckets
		if node, exists := m.storageIndex.GetStorageNode(nodeName); exists {
			updatedNode := &StorageNode{
				Name:              node.Name,
				NodeType:          node.NodeType,
				ServiceType:       StorageServiceMinio,
				Region:            node.Region,
				Zone:              node.Zone,
				CapacityBytes:     node.CapacityBytes,
				AvailableBytes:    node.AvailableBytes,
				StorageTechnology: node.StorageTechnology,
				LastUpdated:       time.Now(),
				Buckets:           nodeBuckets,
				TopologyLabels:    node.TopologyLabels,
			}
			m.storageIndex.RegisterOrUpdateStorageNode(updatedNode)
		}
	}

	m.storageIndex.MarkRefreshed()
	return nil
}

// indexBucketObjects indexes all objects in a bucket
func (m *MinioClient) indexBucketObjects(ctx context.Context, client *minio.Client, bucketName, nodeName string) error {
	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true})

	objectCount := 0
	for object := range objectCh {
		if object.Err != nil {
			klog.Warningf("Error listing object in bucket %s: %v", bucketName, object.Err)
			continue
		}

		urn := fmt.Sprintf("%s/%s", bucketName, object.Key)

		// retrieve object stats/metadata
		objInfo, err := client.StatObject(ctx, bucketName, object.Key, minio.StatObjectOptions{})
		if err != nil {
			klog.V(4).Infof("Failed to get stats for object %s: %v", urn, err)
			continue
		}

		dataItem, exists := m.storageIndex.GetDataItem(urn)
		var locations []string

		if exists {
			locations = dataItem.Locations
			if !containsString(locations, nodeName) {
				locations = append(locations, nodeName)
			}
		} else {
			locations = []string{nodeName}
		}

		m.storageIndex.AddDataItem(&DataItem{
			URN:          urn,
			Size:         objInfo.Size,
			Locations:    locations,
			LastModified: objInfo.LastModified,
			ContentType:  objInfo.ContentType,
			Metadata:     objInfo.UserMetadata,
		})

		objectCount++
	}

	klog.V(4).Infof("Indexed %d objects in bucket %s on node %s", objectCount, bucketName, nodeName)
	return nil
}

func (m *MinioClient) RunPeriodicRefresh(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.DiscoverMinioInstances(ctx); err != nil {
				klog.Warningf("Failed to discover MinIO instances: %v", err)
			}
			if err := m.IndexAllBuckets(ctx); err != nil {
				klog.Warningf("Failed to index MinIO buckets: %v", err)
			}
		case <-ctx.Done():
			klog.Info("Stopping MinIO client refresh loop")
			return
		}
	}
}

// Helper function to check if a string is in a slice
func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}
