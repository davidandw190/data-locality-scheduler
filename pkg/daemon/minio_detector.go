package daemon

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage/minio"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type MinioDetector struct {
	clientset  kubernetes.Interface
	nodeName   string
	indexer    *minio.Indexer
	minioPorts []int
}

func NewMinioDetector(clientset kubernetes.Interface, nodeName string, indexer *minio.Indexer) *MinioDetector {
	return &MinioDetector{
		clientset:  clientset,
		nodeName:   nodeName,
		indexer:    indexer,
		minioPorts: []int{9000, 9001}, // standard
	}
}

func (d *MinioDetector) DetectLocalMinioService(ctx context.Context) (bool, []string, error) {
	// we find Minio pods running on this node
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", d.nodeName)
	labelSelector := "app=minio"

	pods, err := d.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: labelSelector,
	})

	if err != nil {
		return false, nil, fmt.Errorf("failed to list Minio pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return false, nil, nil
	}

	// use the first Minio pod
	minioPod := pods.Items[0]
	if minioPod.Status.Phase != v1.PodRunning {
		return false, nil, fmt.Errorf("minio pod %s/%s is not running", minioPod.Namespace, minioPod.Name)
	}

	podIP := minioPod.Status.PodIP
	if podIP == "" {
		return false, nil, fmt.Errorf("minio pod %s/%s has no IP address", minioPod.Namespace, minioPod.Name)
	}

	port := 9000
	for _, container := range minioPod.Spec.Containers {
		for _, containerPort := range container.Ports {
			if containerPort.ContainerPort == 9000 {
				port = int(containerPort.ContainerPort)
				break
			}
		}
	}

	endpoint := fmt.Sprintf("%s:%d", podIP, port)

	d.indexer.RegisterMinioNode(d.nodeName, endpoint, false)

	client := minio.NewClient(endpoint, false)
	if err := client.Connect(); err != nil {
		return true, nil, fmt.Errorf("failed to connect to Minio at %s: %w", endpoint, err)
	}

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		return true, nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	klog.V(2).Infof("Detected Minio on node %s with %d buckets", d.nodeName, len(buckets))
	return true, buckets, nil
}

func (d *MinioDetector) AddMinioLabelsToNode(labels map[string]string, buckets []string) {
	labels[StorageNodeLabel] = "true"
	labels[StorageTypeLabel] = "object"
	labels[StorageTechLabel] = "minio"

	// bucket labels
	for _, bucket := range buckets {
		labels[BucketLabelPrefix+bucket] = "true"
	}

	// storage capacity if available
	labels[StorageCapacityLabel] = strconv.FormatInt(d.getStorageCapacity(), 10)
}

func (d *MinioDetector) getStorageCapacity() int64 {
	pvcs, err := d.clientset.CoreV1().PersistentVolumeClaims("").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=minio",
	})

	if err != nil {
		klog.Warningf("Failed to get Minio PVCs: %v", err)
		return 0
	}

	var totalCapacity int64
	for _, pvc := range pvcs.Items {
		if capacity, exists := pvc.Status.Capacity["storage"]; exists {
			totalCapacity += capacity.Value()
		}
	}

	if totalCapacity > 0 {
		return totalCapacity
	}

	// fallback - use node allocatable storage
	node, err := d.clientset.CoreV1().Nodes().Get(context.Background(), d.nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s: %v", d.nodeName, err)
		return 0
	}

	return node.Status.Allocatable.Storage().Value()
}

func (d *MinioDetector) isPortOpen(host string, port int) bool {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
