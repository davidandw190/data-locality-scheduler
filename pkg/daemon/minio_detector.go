package daemon

import (
	"context"
	"fmt"
	"strconv"

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
	labelSelector := "app in (minio, minio-edge)"

	pods, err := d.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: labelSelector,
	})

	if err != nil {
		return false, nil, fmt.Errorf("failed to list Minio pods: %w", err)
	}

	if len(pods.Items) == 0 {
		klog.V(4).Infof("No MinIO pods found on node %s", d.nodeName)
		return false, nil, nil
	}

	var runningPods []v1.Pod
	for _, pod := range pods.Items {
		if pod.Status.Phase == v1.PodRunning {
			runningPods = append(runningPods, pod)
		}
	}

	if len(runningPods) == 0 {
		klog.V(3).Infof("No running MinIO pods found on node %s", d.nodeName)
		return false, nil, nil
	}

	minioPod := runningPods[0]
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

	//register this specific node's MinIO endpoint
	d.indexer.RegisterMinioNode(d.nodeName, endpoint, false)

	// also register service associations
	serviceName := minioPod.Labels["app"]
	if region, hasRegion := minioPod.Labels["region"]; hasRegion {
		serviceName = fmt.Sprintf("%s-%s", serviceName, region)
	}

	// associate service with this node
	d.indexer.AssociateServiceWithNode(serviceName, d.nodeName)
	d.indexer.AssociateServiceWithNode(minioPod.Name, d.nodeName)
	d.indexer.RegisterMinioService(serviceName, endpoint, false)

	// connect to the MinIO instance
	client := minio.NewClient(endpoint, false)
	if err := client.Connect(); err != nil {
		klog.Warningf("Failed to connect to Minio at %s on node %s: %v", endpoint, d.nodeName, err)
		return false, nil, err
	}

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		klog.Warningf("Failed to list buckets on node %s: %v", d.nodeName, err)
		return false, nil, err
	}

	if len(buckets) == 0 {
		klog.V(3).Infof("No buckets found in MinIO on node %s", d.nodeName)
		return true, []string{}, nil
	}

	klog.V(2).Infof("Detected MinIO on node %s with %d buckets: %v", d.nodeName, len(buckets), buckets)
	return true, buckets, nil
}

func (d *MinioDetector) AddMinioLabelsToNode(labels map[string]string, buckets []string) {
	if len(buckets) == 0 {
		klog.V(3).Infof("No buckets found in MinIO on node %s, skipping storage labels", d.nodeName)
		return
	}

	node, err := d.clientset.CoreV1().Nodes().Get(context.Background(), d.nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s: %v", d.nodeName, err)
		return
	}

	region := node.Labels["topology.kubernetes.io/region"]
	zone := node.Labels["topology.kubernetes.io/zone"]
	if region == "" || zone == "" {
		klog.Warningf("Node %s missing region/zone information, storage functionality may be limited", d.nodeName)
	}

	labels[StorageNodeLabel] = "true"
	labels[StorageTypeLabel] = "object"
	labels[StorageTechLabel] = "minio"

	klog.Infof("Node %s hosts the following buckets:", d.nodeName)
	for _, bucket := range buckets {
		labels[BucketLabelPrefix+bucket] = "true"

		// also register this bucket with the node in the storage index
		d.indexer.RegisterBucketForNode(bucket, d.nodeName)

		klog.Infof("  - %s", bucket)
	}

	capacity := d.getStorageCapacity()
	if capacity > 0 {
		labels[StorageCapacityLabel] = strconv.FormatInt(capacity, 10)
		klog.Infof("Storage capacity: %d bytes", capacity)
	} else {
		klog.Warningf("Unable to determine storage capacity for MinIO on node %s", d.nodeName)
	}
}

func (d *MinioDetector) getStorageCapacity() int64 {
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", d.nodeName)
	labelSelector := "app in (minio, minio-edge)"
	pods, err := d.clientset.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: labelSelector,
	})

	if err != nil {
		klog.Warningf("Failed to get MinIO pods for capacity detection: %v", err)
	} else if len(pods.Items) > 0 {
		for _, pod := range pods.Items {
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim != nil {
					pvc, err := d.clientset.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(
						context.Background(), volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					if err != nil {
						klog.V(4).Infof("Failed to get PVC %s: %v", volume.PersistentVolumeClaim.ClaimName, err)
						continue
					}
					if capacity, exists := pvc.Status.Capacity["storage"]; exists {
						capValue := capacity.Value()
						klog.V(3).Infof("Found MinIO storage capacity from PVC: %d bytes", capValue)
						return capValue
					}
				}
			}
		}
	}

	pvcs, err := d.clientset.CoreV1().PersistentVolumeClaims("").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app in (minio, minio-edge)",
	})

	if err != nil {
		klog.Warningf("Failed to list MinIO PVCs: %v", err)
	} else {
		var totalCapacity int64
		for _, pvc := range pvcs.Items {
			if capacity, exists := pvc.Status.Capacity["storage"]; exists {
				totalCapacity += capacity.Value()
			}
		}

		if totalCapacity > 0 {
			klog.V(3).Infof("Found MinIO storage capacity from labeled PVCs: %d bytes", totalCapacity)
			return totalCapacity
		}
	}

	// as a last resort: use a reasonable portion of node storage capacity
	// but only if we're confident this is a storage node
	node, err := d.clientset.CoreV1().Nodes().Get(context.Background(), d.nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s: %v", d.nodeName, err)
		return 0
	}

	allocatable := node.Status.Allocatable.Storage().Value()
	if allocatable > 1024*1024*1024 { // > 1GB
		// fallback - 80% of allocatable storage
		estimatedCapacity := int64(float64(allocatable) * 0.8)
		klog.V(3).Infof("Estimated MinIO storage capacity from node allocatable: %d bytes", estimatedCapacity)
		return estimatedCapacity
	}

	klog.Warningf("Could not determine MinIO storage capacity for node %s", d.nodeName)
	return 0
}
