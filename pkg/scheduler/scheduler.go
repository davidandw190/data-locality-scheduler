package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	"github.com/davidandw190/data-locality-scheduler/pkg/storage/minio"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// Labels
	StorageNodeLabel = "node-capability/storage-service"
	EdgeNodeLabel    = "node-capability/node-type"
	EdgeNodeValue    = "edge"
	CloudNodeValue   = "cloud"
	RegionLabel      = "topology.kubernetes.io/region"
	ZoneLabel        = "topology.kubernetes.io/zone"

	// Default settings
	schedulerInterval      = 1 * time.Second
	storageRefreshInterval = 5 * time.Minute
)

type NodeScore struct {
	Name  string
	Score int
}

type nodeResources struct {
	usedCPU    int64
	usedMemory int64
	timestamp  time.Time
}

type schedulerMetrics struct {
	schedulingLatency  prometheus.Histogram
	schedulingAttempts prometheus.Counter
	schedulingFailures prometheus.Counter
	cacheHits          prometheus.Counter
	cacheMisses        prometheus.Counter
}

type Scheduler struct {
	clientset            kubernetes.Interface
	schedulerName        string
	podQueue             chan *v1.Pod
	storageIndex         *storage.StorageIndex
	bandwidthGraph       *storage.BandwidthGraph
	storageMutex         sync.RWMutex
	priorityFuncs        []PriorityFunc
	nodeResourceCache    map[string]*nodeResources
	cacheLock            sync.RWMutex
	dataLocalityPriority *DataLocalityPriority
	enableMockData       bool
	metrics              schedulerMetrics
	recorder             record.EventRecorder
	retryCount           map[string]int
	retryMutex           sync.Mutex
	cacheExpiration      time.Duration
}

type PriorityFunc func(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error)

func NewScheduler(clientset kubernetes.Interface, schedulerName string) *Scheduler {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: clientset.CoreV1().Events(""),
		},
	)

	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		v1.EventSource{Component: "data-locality-scheduler"},
	)

	return &Scheduler{
		clientset:         clientset,
		schedulerName:     schedulerName,
		podQueue:          make(chan *v1.Pod, 100),
		storageIndex:      storage.NewStorageIndex(),
		bandwidthGraph:    storage.NewBandwidthGraph(50 * 1024 * 1024), // 50 MB/s default
		priorityFuncs:     make([]PriorityFunc, 0),
		nodeResourceCache: make(map[string]*nodeResources),
		enableMockData:    false,
		metrics: schedulerMetrics{
			schedulingLatency: promauto.NewHistogram(prometheus.HistogramOpts{
				Name:    "data_locality_scheduler_latency_seconds",
				Help:    "Scheduling latency in seconds",
				Buckets: prometheus.DefBuckets,
			}),
			schedulingAttempts: promauto.NewCounter(prometheus.CounterOpts{
				Name: "data_locality_scheduler_attempts_total",
				Help: "Total scheduling attempts",
			}),
			schedulingFailures: promauto.NewCounter(prometheus.CounterOpts{
				Name: "data_locality_scheduler_failures_total",
				Help: "Total scheduling failures",
			}),
			cacheHits: promauto.NewCounter(prometheus.CounterOpts{
				Name: "data_locality_scheduler_cache_hits_total",
				Help: "Total resource cache hits",
			}),
			cacheMisses: promauto.NewCounter(prometheus.CounterOpts{
				Name: "data_locality_scheduler_cache_misses_total",
				Help: "Total resource cache misses",
			}),
		},
		recorder:        recorder,
		retryCount:      make(map[string]int),
		cacheExpiration: 30 * time.Second,
	}
}

func (s *Scheduler) SetStorageIndex(idx *storage.StorageIndex) {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	s.storageIndex = idx
}

func (s *Scheduler) SetBandwidthGraph(graph *storage.BandwidthGraph) {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	s.bandwidthGraph = graph
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.initPriorityFunctions()
	s.dataLocalityPriority = NewDataLocalityPriority(s.storageIndex, s.bandwidthGraph)

	podInformer := s.createPodInformer(ctx)
	go podInformer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for pod informer caches to sync")
	}

	if err := s.initStorageInformation(ctx); err != nil {
		klog.Warningf("Failed to initialize storage information: %v", err)
		klog.Warningf("Continuing with limited storage awareness")
	}

	if err := s.discoverAndRegisterMinioServices(ctx); err != nil {
		klog.Warningf("Failed to discover MinIO services: %v", err)
		klog.Warningf("Continuing with limited MinIO awareness")
	} else {
		klog.Info("Successfully initialized MinIO service discovery")
	}

	s.storageIndex.PerformMaintenance()
	klog.Info("Initial storage index maintenance complete")

	if s.enableMockData {
		klog.Info("Creating mock storage data for testing")
		s.storageIndex.MockMinioData()
		s.bandwidthGraph.MockNetworkPaths()
	} else {
		klog.Info("Mock data creation disabled, using only real storage detection")
	}

	go s.refreshStorageDataPeriodically(ctx)
	go s.startHealthCheckServer(ctx)
	go wait.UntilWithContext(ctx, s.scheduleOne, schedulerInterval)

	klog.Infof("Scheduler %s started successfully", s.schedulerName)
	<-ctx.Done()
	klog.Info("Scheduler shutting down")
	return nil
}

func (s *Scheduler) initPriorityFunctions() {
	s.priorityFuncs = append(s.priorityFuncs, s.scoreResourcePriority)
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeAffinity)
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeType)
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeCapabilities)
}

func (s *Scheduler) initStorageInformation(ctx context.Context) error {
	klog.Info("Initializing storage information")

	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	var storageNodes []*storage.StorageNode
	var edgeNodes []string
	var cloudNodes []string
	var regions []string
	var zones []string

	for _, node := range nodes.Items {
		region := node.Labels[RegionLabel]
		zone := node.Labels[ZoneLabel]

		nodeType := storage.StorageTypeCloud // Default to cloud
		if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
			nodeType = storage.StorageTypeEdge
			edgeNodes = append(edgeNodes, node.Name)
		} else {
			cloudNodes = append(cloudNodes, node.Name)
		}

		if region != "" && !containsString(regions, region) {
			regions = append(regions, region)
		}

		if zone != "" && !containsString(zones, zone) {
			zones = append(zones, zone)
		}

		s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

		if value, ok := node.Labels[StorageNodeLabel]; ok && value == "true" {
			storageType := "generic"
			if t, ok := node.Labels["node-capability/storage-type"]; ok {
				storageType = t
			}

			var capacity int64
			if capStr, ok := node.Labels["node-capability/storage-capacity-bytes"]; ok {
				capacity, _ = strconv.ParseInt(capStr, 10, 64)
			}

			if capacity == 0 {
				capacity = node.Status.Capacity.Storage().Value()
			}

			storageTech := "unknown"
			if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
				storageTech = tech
			}

			var buckets []string
			for label := range node.Labels {
				if strings.HasPrefix(label, "node-capability/storage-bucket-") {
					bucket := strings.TrimPrefix(label, "node-capability/storage-bucket-")
					buckets = append(buckets, bucket)
				}
			}

			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceType(storageType),
				Region:            region,
				Zone:              zone,
				CapacityBytes:     capacity,
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: storageTech,
				LastUpdated:       time.Now(),
				Buckets:           buckets,
				TopologyLabels:    make(map[string]string),
			}

			// for tracking capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			storageNodes = append(storageNodes, storageNode)
		} else {
			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceGeneric,
				Region:            region,
				Zone:              zone,
				CapacityBytes:     node.Status.Capacity.Storage().Value(),
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: "unknown",
				LastUpdated:       time.Now(),
				Buckets:           []string{},
				TopologyLabels:    make(map[string]string),
			}

			// for tracking capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			storageNodes = append(storageNodes, storageNode)
		}
	}

	s.storageMutex.Lock()
	for _, node := range storageNodes {
		s.storageIndex.RegisterOrUpdateStorageNode(node)

		for _, bucket := range node.Buckets {
			bucketNodes := s.storageIndex.GetBucketNodes(bucket)
			if bucketNodes == nil {
				bucketNodes = []string{node.Name}
			} else if !containsString(bucketNodes, node.Name) {
				bucketNodes = append(bucketNodes, node.Name)
			}
			s.storageIndex.RegisterBucket(bucket, bucketNodes)
		}
	}
	s.storageMutex.Unlock()
	s.initBandwidthInformation(nodes.Items)

	klog.Infof("Storage initialization complete: %d storage nodes, %d edge nodes, %d cloud nodes, %d regions, %d zones",
		len(storageNodes), len(edgeNodes), len(cloudNodes), len(regions), len(zones))

	return nil
}

func (s *Scheduler) initBandwidthInformation(nodes []v1.Node) {
	s.bandwidthGraph.SetTopologyDefaults(
		1e9, 0.1, // local: 1 GB/s, 0.1ms latency
		500e6, 1.0, // same zone: 500 MB/s, 1ms latency
		200e6, 5.0, // same region: 200 MB/s, 5ms latency
		50e6, 20.0, // edge-cloud: 50 MB/s, 20ms latency
	)

	for _, source := range nodes {
		for _, dest := range nodes {
			if source.Name == dest.Name {
				continue
			}

			bandwidthLabel := fmt.Sprintf("node-capability/bandwidth-to-%s", dest.Name)
			if bandwidthStr, ok := source.Labels[bandwidthLabel]; ok {
				if bandwidth, err := strconv.ParseInt(bandwidthStr, 10, 64); err == nil && bandwidth > 0 {
					latencyLabel := fmt.Sprintf("node-capability/latency-to-%s", dest.Name)
					latency := 5.0 // default 5ms
					if latencyStr, ok := source.Labels[latencyLabel]; ok {
						if l, err := strconv.ParseFloat(latencyStr, 64); err == nil && l > 0 {
							latency = l
						}
					}

					s.bandwidthGraph.SetBandwidth(source.Name, dest.Name, float64(bandwidth), latency)
				}
			}
		}
	}
}

func (s *Scheduler) refreshStorageDataPeriodically(ctx context.Context) {
	ticker := time.NewTicker(storageRefreshInterval)
	defer ticker.Stop()

	maintenanceTicker := time.NewTicker(storageRefreshInterval * 4)
	defer maintenanceTicker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.refreshStorageInformation(ctx); err != nil {
				klog.Warningf("Failed to refresh storage information: %v", err)
			}
		case <-ctx.Done():
			klog.Info("Stopping storage refresh loop")
			return
		}
	}
}

func (s *Scheduler) refreshStorageInformation(ctx context.Context) error {
	klog.V(4).Info("Refreshing storage information")

	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	existingStorageNodes := make(map[string]bool)

	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	for _, node := range nodes.Items {
		if value, ok := node.Labels[StorageNodeLabel]; ok && value == "true" {
			existingStorageNodes[node.Name] = true

			nodeType := storage.StorageTypeCloud
			if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
				nodeType = storage.StorageTypeEdge
			}

			storageType := "generic"
			if t, ok := node.Labels["node-capability/storage-type"]; ok {
				storageType = t
			}

			region := node.Labels[RegionLabel]
			zone := node.Labels[ZoneLabel]
			s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

			var capacity int64
			if capStr, ok := node.Labels["node-capability/storage-capacity-bytes"]; ok {
				capacity, _ = strconv.ParseInt(capStr, 10, 64)
			}

			if capacity == 0 {
				capacity = node.Status.Capacity.Storage().Value()
			}

			storageTech := "unknown"
			if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
				storageTech = tech
			}

			var buckets []string
			for label, value := range node.Labels {
				if strings.HasPrefix(label, "node-capability/storage-bucket-") && value == "true" {
					bucket := strings.TrimPrefix(label, "node-capability/storage-bucket-")
					buckets = append(buckets, bucket)
				}
			}

			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceType(storageType),
				Region:            region,
				Zone:              zone,
				CapacityBytes:     capacity,
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: storageTech,
				LastUpdated:       time.Now(),
				Buckets:           buckets,
				TopologyLabels:    make(map[string]string),
			}

			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			s.storageIndex.RegisterOrUpdateStorageNode(storageNode)

			for _, bucket := range buckets {
				bucketNodes := s.storageIndex.GetBucketNodes(bucket)
				if bucketNodes == nil {
					bucketNodes = []string{node.Name}
				} else if !containsString(bucketNodes, node.Name) {
					bucketNodes = append(bucketNodes, node.Name)
				}
				s.storageIndex.RegisterBucket(bucket, bucketNodes)
			}
		} else {
			nodeType := storage.StorageTypeCloud
			if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
				nodeType = storage.StorageTypeEdge
			}

			region := node.Labels[RegionLabel]
			zone := node.Labels[ZoneLabel]
			s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceGeneric,
				Region:            region,
				Zone:              zone,
				CapacityBytes:     node.Status.Capacity.Storage().Value(),
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: "unknown",
				LastUpdated:       time.Now(),
				Buckets:           []string{},
				TopologyLabels:    make(map[string]string),
			}

			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			s.storageIndex.RegisterOrUpdateStorageNode(storageNode)
		}

		for _, dest := range nodes.Items {
			if node.Name == dest.Name {
				continue
			}

			bandwidthLabel := fmt.Sprintf("node-capability/bandwidth-to-%s", dest.Name)
			if bandwidthStr, ok := node.Labels[bandwidthLabel]; ok {
				if bandwidth, err := strconv.ParseInt(bandwidthStr, 10, 64); err == nil && bandwidth > 0 {
					latencyLabel := fmt.Sprintf("node-capability/latency-to-%s", dest.Name)
					latency := 5.0 // default 5ms
					if latencyStr, ok := node.Labels[latencyLabel]; ok {
						if l, err := strconv.ParseFloat(latencyStr, 64); err == nil && l > 0 {
							latency = l
						}
					}

					s.bandwidthGraph.SetBandwidth(node.Name, dest.Name, float64(bandwidth), latency)
				}
			}
		}
	}

	// remove nodes that no longer exist or are no longer storage nodes
	currentStorageNodes := s.storageIndex.GetAllStorageNodes()
	for _, node := range currentStorageNodes {
		if node.ServiceType == storage.StorageServiceMinio && !existingStorageNodes[node.Name] {
			s.storageIndex.RemoveStorageNode(node.Name)
			s.bandwidthGraph.RemoveNode(node.Name)
		}
	}

	s.storageIndex.PruneStaleBuckets()
	s.storageIndex.PruneStaleDataItems()
	s.storageIndex.MarkRefreshed()

	maintenanceStart := time.Now()
	s.storageIndex.PerformMaintenance()

	klog.V(3).Infof("Storage maintenance completed in %v",
		time.Since(maintenanceStart))

	klog.V(4).Info("Storage refresh complete")
	return nil
}

func (s *Scheduler) SetEnableMockData(enable bool) {
	s.enableMockData = enable
}

func (s *Scheduler) discoverAndRegisterMinioServices(ctx context.Context) error {
	klog.Info("Discovering and registering MinIO services...")
	minioIndexer := minio.NewIndexer(s.storageIndex, 5*time.Minute)

	if err := minioIndexer.DiscoverMinioNodesFromKubernetes(ctx, s.clientset); err != nil {
		klog.Warningf("Failed to discover MinIO nodes from Kubernetes: %v", err)
	}

	if err := minioIndexer.DiscoverMinioServicesFromKubernetes(ctx, s.clientset); err != nil {
		klog.Warningf("Failed to discover MinIO services from Kubernetes: %v", err)
	}

	if err := minioIndexer.RefreshIndex(ctx); err != nil {
		klog.Warningf("Failed to refresh MinIO index: %v", err)
	}

	s.storageMutex.RLock()
	dataItemCount := len(s.storageIndex.GetAllDataItems())
	s.storageMutex.RUnlock()

	klog.Infof("Initial MinIO discovery found %d data items", dataItemCount)

	if dataItemCount == 0 {
		klog.Info("No data items found, attempting more aggressive discovery...")
		time.Sleep(2 * time.Second)

		serviceNames := []struct {
			name     string
			endpoint string
		}{
			{"minio", "minio.data-locality-scheduler.svc.cluster.local:9000"},
			{"minio-edge-region1", "minio-edge-region1.data-locality-scheduler.svc.cluster.local:9000"},
			{"minio-edge-region2", "minio-edge-region2.data-locality-scheduler.svc.cluster.local:9000"},
		}

		for _, svc := range serviceNames {
			minioIndexer.RegisterMinioService(svc.name, svc.endpoint, false)
		}

		if err := minioIndexer.RefreshIndex(ctx); err != nil {
			klog.Warningf("Failed to refresh MinIO index in second attempt: %v", err)
		}

		s.storageMutex.RLock()
		dataItemCount = len(s.storageIndex.GetAllDataItems())
		s.storageMutex.RUnlock()

		klog.Infof("After aggressive discovery: found %d data items", dataItemCount)
	}

	minioIndexer.StartRefresher(ctx)

	return nil
}

// createPodInformer creates a pod informer to watch for unscheduled pods
func (s *Scheduler) createPodInformer(_ context.Context) cache.Controller {
	// we watch pods that:
	// - use our scheduler name
	// - dont have a node assigned yet
	fieldSelector := fields.SelectorFromSet(fields.Set{
		"spec.schedulerName": s.schedulerName,
		"spec.nodeName":      "",
	})

	podListWatcher := cache.NewListWatchFromClient(
		s.clientset.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		fieldSelector,
	)

	_, informer := cache.NewIndexerInformer(
		podListWatcher,
		&v1.Pod{},
		0, // resync disabled
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod, ok := obj.(*v1.Pod)
				if !ok {
					klog.Errorf("Failed to convert object to Pod")
					return
				}
				s.enqueuePod(pod)
			},
			UpdateFunc: func(_, newObj interface{}) {
				pod, ok := newObj.(*v1.Pod)
				if !ok {
					klog.Errorf("Failed to convert object to Pod")
					return
				}
				s.enqueuePod(pod)
			},
		},
		cache.Indexers{},
	)

	return informer
}

func (s *Scheduler) enqueuePod(pod *v1.Pod) {
	// we skip pods that already have a node or use a different scheduler
	if pod.Spec.NodeName != "" || pod.Spec.SchedulerName != s.schedulerName {
		return
	}

	// we skip pods that are being deleted
	if pod.DeletionTimestamp != nil {
		return
	}

	s.podQueue <- pod
}

func (s *Scheduler) scheduleOne(ctx context.Context) {
	var pod *v1.Pod
	select {
	case pod = <-s.podQueue:
	case <-ctx.Done():
		return
	default:
		return
	}

	startTime := time.Now()
	klog.Infof("Attempting to schedule pod: %s/%s", pod.Namespace, pod.Name)

	nodeName, err := s.findBestNodeForPod(ctx, pod)
	if err != nil {
		s.recordSchedulingFailure(ctx, pod, err)
		return
	}

	err = s.bindPod(ctx, pod, nodeName)
	if err != nil {
		s.recordSchedulingFailure(ctx, pod, fmt.Errorf("binding failed: %w", err))
		return
	}

	latency := time.Since(startTime)
	s.metrics.schedulingLatency.Observe(latency.Seconds())
	s.metrics.schedulingAttempts.Inc()

	klog.Infof("Successfully scheduled pod %s/%s to node %s (took %v)",
		pod.Namespace, pod.Name, nodeName, latency)

	s.recorder.Eventf(pod, v1.EventTypeNormal, "Scheduled",
		"Successfully assigned %s/%s to %s", pod.Namespace, pod.Name, nodeName)
}

func (s *Scheduler) findBestNodeForPod(ctx context.Context, pod *v1.Pod) (string, error) {
	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no nodes available in the cluster")
	}

	filteredNodes, err := s.filterNodes(ctx, pod, nodes.Items)
	if err != nil {
		return "", fmt.Errorf("node filtering error: %w", err)
	}

	if len(filteredNodes) == 0 {
		return "", fmt.Errorf("no suitable nodes found after filtering")
	}

	nodeScores, err := s.prioritizeNodes(pod, filteredNodes)
	if err != nil {
		return "", fmt.Errorf("node prioritization error: %w", err)
	}

	if len(nodeScores) == 0 {
		return "", fmt.Errorf("no suitable nodes found after scoring")
	}

	sort.Slice(nodeScores, func(i, j int) bool {
		return nodeScores[i].Score > nodeScores[j].Score
	})

	if klog.V(4).Enabled() {
		topN := 3
		if len(nodeScores) < topN {
			topN = len(nodeScores)
		}

		klog.V(4).Infof("Top %d nodes for pod %s/%s:", topN, pod.Namespace, pod.Name)
		for i := 0; i < topN; i++ {
			klog.V(4).Infof("  %d. Node: %s, Score: %d", i+1, nodeScores[i].Name, nodeScores[i].Score)
		}
	}

	return nodeScores[0].Name, nil
}

func (s *Scheduler) recordSchedulingFailure(ctx context.Context, pod *v1.Pod, err error) {
	klog.Errorf("Failed to schedule pod %s/%s: %v", pod.Namespace, pod.Name, err)

	s.metrics.schedulingFailures.Inc()

	s.recorder.Eventf(pod, v1.EventTypeWarning, "FailedScheduling",
		"Failed to schedule pod: %v", err)

	if updateErr := s.updatePodSchedulingStatus(ctx, pod, err.Error()); updateErr != nil {
		klog.Warningf("Failed to update status for pod %s/%s: %v",
			pod.Namespace, pod.Name, updateErr)
	}

	// reequeue the pod for later scheduling attempts (with backoff)
	go func() {
		retryCount, ok := s.retryCount[string(pod.UID)]
		if !ok {
			retryCount = 0
		}

		backoff := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
		if backoff > 60*time.Second {
			backoff = 60 * time.Second // cap at 1 minute
		}

		s.retryCount[string(pod.UID)] = retryCount + 1

		time.Sleep(backoff)
		s.podQueue <- pod
	}()
}

func (s *Scheduler) updatePodSchedulingStatus(ctx context.Context, pod *v1.Pod, message string) error {
	podCopy := pod.DeepCopy()

	now := metav1.Now()
	condition := v1.PodCondition{
		Type:               v1.PodScheduled,
		Status:             v1.ConditionFalse,
		LastProbeTime:      now,
		LastTransitionTime: now,
		Reason:             "SchedulerError",
		Message:            message,
	}

	found := false
	for i, podCondition := range podCopy.Status.Conditions {
		if podCondition.Type == v1.PodScheduled {
			podCopy.Status.Conditions[i] = condition
			found = true
			break
		}
	}

	if !found {
		podCopy.Status.Conditions = append(podCopy.Status.Conditions, condition)
	}

	_, err := s.clientset.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, podCopy, metav1.UpdateOptions{})
	return err
}

func (s *Scheduler) filterNodes(ctx context.Context, pod *v1.Pod, nodes []v1.Node) ([]v1.Node, error) {
	startTime := time.Now()
	nodeCount := len(nodes)

	filteredNodes := make([]v1.Node, 0, nodeCount/2+1)

	if nodeCount == 0 {
		return filteredNodes, nil
	}

	filterReasons := make(map[string]int)

	for _, node := range nodes {
		if !isNodeReady(&node) {
			filterReasons["NotReady"]++
			continue
		}

		if !s.nodeFitsResources(ctx, pod, &node) {
			filterReasons["InsufficientResources"]++
			continue
		}

		if !s.nodeHasRequiredCapabilities(pod, &node) {
			filterReasons["MissingCapabilities"]++
			continue
		}

		if !s.satisfiesNodeAffinity(pod, &node) {
			filterReasons["NodeAffinityMismatch"]++
			continue
		}

		if !s.toleratesNodeTaints(pod, &node) {
			filterReasons["TaintNotTolerated"]++
			continue
		}

		filteredNodes = append(filteredNodes, node)
	}

	if klog.V(4).Enabled() {
		klog.V(4).Infof("Node filtering for pod %s/%s: started with %d nodes, filtered to %d nodes in %v",
			pod.Namespace, pod.Name, nodeCount, len(filteredNodes), time.Since(startTime))

		for reason, count := range filterReasons {
			klog.V(4).Infof("- Filtered out %d nodes due to %s", count, reason)
		}
	}

	return filteredNodes, nil
}
func (s *Scheduler) prioritizeNodes(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var allScores [][]NodeScore

	for _, priorityFunc := range s.priorityFuncs {
		scores, err := priorityFunc(pod, nodes)
		if err != nil {
			klog.Warningf("Error applying priority function: %v", err)
			continue
		}
		allScores = append(allScores, scores)
	}

	s.storageMutex.RLock()
	if s.dataLocalityPriority != nil {
		var dataLocalityScores []NodeScore

		for _, node := range nodes {
			score, err := s.dataLocalityPriority.Score(pod, node.Name)
			if err != nil {
				klog.Warningf("Error calculating data locality score for pod %s/%s on node %s: %v",
					pod.Namespace, pod.Name, node.Name, err)
				score = 50
			}

			dataLocalityScores = append(dataLocalityScores, NodeScore{
				Name:  node.Name,
				Score: score,
			})
		}

		allScores = append(allScores, dataLocalityScores)
	}
	s.storageMutex.RUnlock()

	return s.combineScores(pod, nodes, allScores), nil
}

func (s *Scheduler) combineScores(pod *v1.Pod, nodes []v1.Node, scoresList [][]NodeScore) []NodeScore {
	weights := s.getWeightsForPod(pod)
	nodeCount := len(nodes)

	if nodeCount == 0 {
		return []NodeScore{}
	}

	nodeIndices := make(map[string]int, nodeCount)
	for i, node := range nodes {
		nodeIndices[node.Name] = i
	}

	normalizedScores := make([][]float64, len(scoresList))
	for i, scores := range scoresList {
		// find min and max scores for this criterion
		minScore := MaxScore
		maxScore := MinScore

		for _, score := range scores {
			if score.Score < minScore {
				minScore = score.Score
			}
			if score.Score > maxScore {
				maxScore = score.Score
			}
		}

		// handle case where all scores are the same
		scoreDiff := maxScore - minScore
		if scoreDiff == 0 {
			scoreDiff = 1
		}

		// normalize scores to 0-1 range
		normalizedScores[i] = make([]float64, nodeCount)
		for _, score := range scores {
			index, exists := nodeIndices[score.Name]
			if exists {
				normalizedScores[i][index] = float64(score.Score-minScore) / float64(scoreDiff)
			}
		}
	}

	// compute weighted sum for each node
	finalScores := make([]float64, nodeCount)
	weightSum := 0.0

	for i, normalizedScore := range normalizedScores {
		weight := 1.0
		if i < len(weights) {
			weight = weights[i]
		}
		weightSum += weight

		for j := 0; j < nodeCount; j++ {
			finalScores[j] += normalizedScore[j] * weight
		}
	}

	// we normalize to account for variable number of criteria
	if weightSum > 0 {
		for j := 0; j < nodeCount; j++ {
			finalScores[j] /= weightSum
		}
	}

	result := make([]NodeScore, nodeCount)
	for j := 0; j < nodeCount; j++ {
		result[j] = NodeScore{
			Name:  nodes[j].Name,
			Score: int(finalScores[j] * MaxScore),
		}
	}

	return result
}

func (s *Scheduler) getWeightsForPod(pod *v1.Pod) []float64 {
	weights := []float64{
		DefaultResourceWeight,
		DefaultNodeAffinityWeight,
		DefaultNodeTypeWeight,
		DefaultCapabilitiesWeight,
		DefaultDataLocalityWeight,
	}

	if pod.Annotations == nil {
		return weights
	}

	if _, ok := pod.Annotations[AnnotationDataIntensive]; ok {
		weights = []float64{
			DataIntensiveResourceWeight,
			DataIntensiveNodeAffinityWeight,
			DataIntensiveNodeTypeWeight,
			DataIntensiveCapabilitiesWeight,
			DataIntensiveDataLocalityWeight, // higher
		}
	} else if _, ok := pod.Annotations[AnnotationComputeIntensive]; ok {
		weights = []float64{
			ComputeIntensiveResourceWeight, // higher
			ComputeIntensiveNodeAffinityWeight,
			ComputeIntensiveNodeTypeWeight,
			ComputeIntensiveCapabilitiesWeight,
			ComputeIntensiveDataLocalityWeight, // lower
		}
	}

	if _, ok := pod.Annotations[AnnotationPreferEdge]; ok {
		weights[2] = 0.3
	}

	return weights
}

// normalizeScores normalizes scores to a 0-100 range
func normalizeScores(scores []NodeScore) []NodeScore {
	maxScore := 0
	for _, score := range scores {
		if score.Score > maxScore {
			maxScore = score.Score
		}
	}

	normalizedScores := make([]NodeScore, len(scores))
	for i, score := range scores {
		normalizedScore := 0
		if maxScore > 0 {
			normalizedScore = score.Score * 100 / maxScore
		}
		normalizedScores[i] = NodeScore{
			Name:  score.Name,
			Score: normalizedScore,
		}
	}

	return normalizedScores
}

// scoreResourcePriority scores nodes based on resource availability
func (s *Scheduler) scoreResourcePriority(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // default 100m CPU
		}

		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // default 200Mi memory
		}
	}

	for _, node := range nodes {
		resourceScore := s.calculateBalancedResourceScore(pod, &node, requestedCPU, requestedMemory)
		resourceScore = s.adjustScoreForSpecialResources(resourceScore, pod, &node)

		if resourceScore > MaxScore {
			resourceScore = MaxScore
		} else if resourceScore < MinScore {
			resourceScore = MinScore
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: resourceScore,
		})
	}

	return scores, nil
}

func (s *Scheduler) calculateBalancedResourceScore(pod *v1.Pod, node *v1.Node,
	requestedCPU, requestedMemory int64) int {

	if computeScoreStr, exists := node.Labels["node-capability/compute-score"]; exists {
		if computeScore, err := strconv.Atoi(computeScoreStr); err == nil {
			if memoryScoreStr, exists := node.Labels["node-capability/memory-score"]; exists {
				if memoryScore, err := strconv.Atoi(memoryScoreStr); err == nil {
					return (computeScore + memoryScore) / 2
				}
			}
			return computeScore
		}
	}

	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	if allocatableCPU == 0 {
		allocatableCPU = 1
	}
	if allocatableMemory == 0 {
		allocatableMemory = 1
	}

	cpuFraction := float64(requestedCPU) / float64(allocatableCPU)
	memoryFraction := float64(requestedMemory) / float64(allocatableMemory)

	//penalize nodes that don't have enough resources
	if cpuFraction >= 1 || memoryFraction >= 1 {
		return MinScore
	}

	// score based on balanced resource usage
	// the closer the CPU and memory fractions are, the better
	diff := math.Abs(cpuFraction - memoryFraction)
	balance := 1 - diff

	// also consider the absolute resource usage
	// less usage is better, but we want to discourage wasting resources
	usage := (cpuFraction + memoryFraction) / 2
	usageScore := 1.0

	// if usage is very low, slightly penalize to avoid wasting resources on large nodes
	if usage < 0.1 {
		usageScore = 0.9 + usage
	} else if usage > 0.7 {
		// if usage is high, penalize more heavily
		usageScore = 1.0 - ((usage - 0.7) * 2)
	}

	finalScore := int((balance*0.7 + usageScore*0.3) * MaxScore)

	if finalScore > MaxScore {
		finalScore = MaxScore
	} else if finalScore < MinScore {
		finalScore = MinScore
	}

	return finalScore
}

// adjustScoreForSpecialResources adjusts resource score based on special requirements
func (s *Scheduler) adjustScoreForSpecialResources(baseScore int, pod *v1.Pod, node *v1.Node) int {
	adjustedScore := baseScore

	// GPU scoring
	if hasGPUCapability(node) {
		if podNeedsGPU(pod) {
			// bonus for having a GPU when required
			adjustedScore += 20
		} else {
			// small penalty for wasting GPU resources
			adjustedScore -= 5
		}
	} else if podNeedsGPU(pod) {
		// major penalty for not having GPU when required
		adjustedScore -= 50
	}

	// Fast storage scoring
	if hasFastStorage(node) {
		if podNeedsFastStorage(pod) {
			// Bonus for having fast storage when required
			adjustedScore += 15
		}
	} else if podNeedsFastStorage(pod) {
		// Penalty for not having fast storage when required
		adjustedScore -= 30
	}

	return adjustedScore
}

func (s *Scheduler) calculateResourceScore(pod *v1.Pod, node *v1.Node) int {
	if computeScoreStr, exists := node.Labels["node-capability/compute-score"]; exists {
		if computeScore, err := strconv.Atoi(computeScoreStr); err == nil {
			return computeScore
		}
	}

	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // default 100m CPU
		}

		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // default 200Mi
		}
	}

	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	cpuRatio := float64(requestedCPU) / float64(allocatableCPU)
	memoryRatio := float64(requestedMemory) / float64(allocatableMemory)

	diff := math.Abs(cpuRatio - memoryRatio)

	score := int((1 - diff) * 100)

	if hasGPUCapability(node) {
		if podNeedsGPU(pod) {
			score += 20
		}
	}

	if hasFastStorage(node) {
		if podNeedsFastStorage(pod) {
			score += 10
		}
	}

	if score > 100 {
		score = 100
	} else if score < 0 {
		score = 0
	}

	return score
}

func (s *Scheduler) scoreNodeAffinity(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
		for _, node := range nodes {
			scores = append(scores, NodeScore{
				Name:  node.Name,
				Score: 50, // neutral score
			})
		}
		return scores, nil
	}

	preferences := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	for _, node := range nodes {
		totalScore := 0
		maxPossibleScore := 0

		for _, preference := range preferences {
			weight := preference.Weight
			maxPossibleScore += int(weight)

			nodeSelector := preference.Preference
			if s.nodeSelectorMatches(&node, &nodeSelector) {
				totalScore += int(weight)
			}
		}

		normalizedScore := 0
		if maxPossibleScore > 0 {
			normalizedScore = totalScore * 100 / maxPossibleScore
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: normalizedScore,
		})
	}

	return scores, nil
}

func (s *Scheduler) scoreNodeType(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	preferEdge := false
	preferCloud := false
	preferredRegion := ""
	preferredZone := ""

	if pod.Annotations != nil {
		_, preferEdge = pod.Annotations[AnnotationPreferEdge]
		_, preferCloud = pod.Annotations[AnnotationPreferCloud]

		if regionValue, ok := pod.Annotations[AnnotationPreferRegion]; ok {
			preferredRegion = regionValue
		}

		if zoneValue, ok := pod.Annotations["scheduler.thesis/prefer-zone"]; ok {
			preferredZone = zoneValue
		}
	}

	for _, node := range nodes {
		score := DefaultScore
		scoreFactors := make(map[string]int)

		// score based on node type preference
		isEdge := isEdgeNode(&node)
		scoreFactors["nodeType"] = DefaultScore

		if preferEdge && isEdge {
			scoreFactors["nodeType"] = MaxScore
		} else if preferEdge && !isEdge {
			scoreFactors["nodeType"] = MinScore
		} else if preferCloud && !isEdge {
			scoreFactors["nodeType"] = MaxScore
		} else if preferCloud && isEdge {
			scoreFactors["nodeType"] = MinScore
		}

		// score based on region/zone preference
		nodeRegion := node.Labels[RegionLabel]
		nodeZone := node.Labels[ZoneLabel]

		scoreFactors["region"] = DefaultScore
		if preferredRegion != "" {
			if nodeRegion == preferredRegion {
				scoreFactors["region"] = MaxScore
			} else {
				scoreFactors["region"] = MinScore
			}
		}

		scoreFactors["zone"] = DefaultScore // Default neutral score
		if preferredZone != "" {
			if nodeZone == preferredZone {
				scoreFactors["zone"] = MaxScore
			} else {
				scoreFactors["zone"] = MinScore
			}
		}

		totalWeight := 0
		weightedSum := 0

		if preferEdge || preferCloud {
			weightedSum += scoreFactors["nodeType"] * 2
			totalWeight += 2
		}

		if preferredRegion != "" {
			weightedSum += scoreFactors["region"] * 3
			totalWeight += 3
		}

		if preferredZone != "" {
			weightedSum += scoreFactors["zone"] * 4
			totalWeight += 4
		}

		if totalWeight > 0 {
			score = weightedSum / totalWeight
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return scores, nil
}

func (s *Scheduler) scoreNodeCapabilities(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	requiredCapabilities := extractPodCapabilityRequirements(pod)

	for _, node := range nodes {
		score := 50

		matchCount := 0
		for capability, required := range requiredCapabilities {
			labelKey := fmt.Sprintf("node-capability/%s", capability)
			if value, exists := node.Labels[labelKey]; exists && (value == "true" || value == required) {
				matchCount++
			}
		}

		if len(requiredCapabilities) > 0 {
			score = matchCount * 100 / len(requiredCapabilities)
		}

		if hasGPUCapability(&node) && podNeedsGPU(pod) {
			score += 20
		}

		if hasFastStorage(&node) && podNeedsFastStorage(pod) {
			score += 10
		}

		if score > 100 {
			score = 100
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return scores, nil
}

func (s *Scheduler) nodeFitsResources(ctx context.Context, pod *v1.Pod, node *v1.Node) bool {
	cacheKey := fmt.Sprintf("%s-%s", node.Name, node.ResourceVersion)

	s.cacheLock.RLock()
	cachedResources, found := s.nodeResourceCache[cacheKey]
	s.cacheLock.RUnlock()

	var usedCPU, usedMemory int64

	if !found {
		// cache miss - query the current usage from API server
		fieldSelector := fields.SelectorFromSet(fields.Set{"spec.nodeName": node.Name})
		pods, err := s.clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fieldSelector.String(),
		})

		if err != nil {
			klog.Errorf("Error getting pods on node %s: %v", node.Name, err)
			return false
		}

		// resource usage by pods on this node
		for _, p := range pods.Items {
			if p.DeletionTimestamp != nil {
				continue
			}

			for _, container := range p.Spec.Containers {
				if container.Resources.Requests.Cpu() != nil {
					usedCPU += container.Resources.Requests.Cpu().MilliValue()
				} else {
					usedCPU += 100 // default 100m CPU
				}

				if container.Resources.Requests.Memory() != nil {
					usedMemory += container.Resources.Requests.Memory().Value()
				} else {
					usedMemory += 200 * 1024 * 1024 // default 200Mi memory
				}
			}
		}

		// update cache
		s.cacheLock.Lock()
		s.nodeResourceCache[cacheKey] = &nodeResources{
			usedCPU:    usedCPU,
			usedMemory: usedMemory,
			timestamp:  time.Now(),
		}
		s.cacheLock.Unlock()
	} else {
		// cache hit
		usedCPU = cachedResources.usedCPU
		usedMemory = cachedResources.usedMemory
	}

	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // default 100m CPU
		}

		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // default 200Mi memory
		}
	}

	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	cpuOK := usedCPU+requestedCPU <= allocatableCPU
	memoryOK := usedMemory+requestedMemory <= allocatableMemory

	if !cpuOK || !memoryOK {
		klog.V(5).Infof("Node %s has insufficient resources for pod %s/%s - CPU: %v, Memory: %v",
			node.Name, pod.Namespace, pod.Name, cpuOK, memoryOK)
	}

	return cpuOK && memoryOK
}

func (s *Scheduler) nodeHasRequiredCapabilities(pod *v1.Pod, node *v1.Node) bool {
	if pod.Annotations == nil {
		return true
	}

	if requiredCap, exists := pod.Annotations["scheduler.thesis/required-capability"]; exists {
		capLabel := fmt.Sprintf("node-capability/%s", requiredCap)
		if value, ok := node.Labels[capLabel]; !ok || value != "true" {
			klog.V(5).Infof("Node %s missing required capability %s", node.Name, requiredCap)
			return false
		}
	}

	if _, ok := pod.Annotations[AnnotationRequireGPU]; ok {
		if !hasGPUCapability(node) {
			klog.V(5).Infof("Node %s missing required GPU capability", node.Name)
			return false
		}
	}

	if _, ok := pod.Annotations["scheduler.thesis/requires-local-storage"]; ok {
		if _, hasStorage := node.Labels[StorageNodeLabel]; !hasStorage {
			klog.V(5).Infof("Node %s missing required storage capability", node.Name)
			return false
		}
	}

	if reqCapString, ok := pod.Annotations["scheduler.thesis/capability-requirements"]; ok {
		requirements := strings.Split(reqCapString, ",")
		for _, req := range requirements {
			req = strings.TrimSpace(req)
			if req == "" {
				continue
			}

			// requirements with values (e.g., "gpu-cores=2")
			parts := strings.Split(req, "=")
			capName := parts[0]

			if len(parts) == 1 {
				capLabel := fmt.Sprintf("node-capability/%s", capName)
				if value, ok := node.Labels[capLabel]; !ok || value != "true" {
					klog.V(5).Infof("Node %s missing required capability %s", node.Name, capName)
					return false
				}
			} else if len(parts) == 2 {
				capLabel := fmt.Sprintf("node-capability/%s", capName)
				if value, ok := node.Labels[capLabel]; !ok || value != parts[1] {
					klog.V(5).Infof("Node %s has wrong value for capability %s: want %s, got %s",
						node.Name, capName, parts[1], value)
					return false
				}
			}
		}
	}

	return true
}

func (s *Scheduler) satisfiesNodeAffinity(pod *v1.Pod, node *v1.Node) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return true
	}

	affinity := pod.Spec.Affinity.NodeAffinity

	if affinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelectorTerms := affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(nodeSelectorTerms) > 0 {
			for _, term := range nodeSelectorTerms {
				if s.nodeSelectorMatches(node, &term) {
					return true
				}
			}

			klog.V(5).Infof("Node %s fails to match any node selector terms for pod", node.Name)
			return false
		}
	}

	return true
}

func (s *Scheduler) nodeSelectorMatches(node *v1.Node, selector *v1.NodeSelectorTerm) bool {
	for _, expr := range selector.MatchExpressions {
		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			if !nodeHasValueForKey(node, expr.Key, expr.Values) {
				return false
			}
		case v1.NodeSelectorOpNotIn:
			if nodeHasValueForKey(node, expr.Key, expr.Values) {
				return false
			}
		case v1.NodeSelectorOpExists:
			if !nodeHasLabelKey(node, expr.Key) {
				return false
			}
		case v1.NodeSelectorOpDoesNotExist:
			if nodeHasLabelKey(node, expr.Key) {
				return false
			}
		case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
			if !nodeMatchesNumericComparison(node, expr.Key, expr.Operator, expr.Values) {
				return false
			}
		}
	}

	for _, expr := range selector.MatchFields {
		if !nodeMatchesFieldExpression(node, expr) {
			return false
		}
	}

	return true
}

func nodeMatchesFieldExpression(node *v1.Node, expr v1.NodeSelectorRequirement) bool {
	if expr.Key == "metadata.name" {
		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			for _, value := range expr.Values {
				if node.Name == value {
					return true
				}
			}
			return false
		case v1.NodeSelectorOpNotIn:
			for _, value := range expr.Values {
				if node.Name == value {
					return false
				}
			}
			return true
		case v1.NodeSelectorOpExists:
			return true // name always exists
		case v1.NodeSelectorOpDoesNotExist:
			return false // name always exists
		}
	}

	klog.V(4).Infof("Unsupported field selector expression for field %s", expr.Key)
	return true
}

func (s *Scheduler) toleratesNodeTaints(pod *v1.Pod, node *v1.Node) bool {
	if len(node.Spec.Taints) == 0 {
		return true
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule ||
			taint.Effect == v1.TaintEffectNoExecute {
			if !tolerationsTolerateTaint(pod.Spec.Tolerations, &taint) {
				return false
			}
		}
	}

	return true
}

func (s *Scheduler) bindPod(ctx context.Context, pod *v1.Pod, nodeName string) error {
	klog.Infof("Binding pod %s/%s to node %s", pod.Namespace, pod.Name, nodeName)

	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: v1.ObjectReference{
			Kind:       "Node",
			Name:       nodeName,
			APIVersion: "v1",
		},
	}

	return s.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
}

func (s *Scheduler) startHealthCheckServer(ctx context.Context) {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Storage info endpoint
	mux.HandleFunc("/storage-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		s.storageMutex.RLock()
		defer s.storageMutex.RUnlock()

		report := map[string]interface{}{
			"storageNodes": len(s.storageIndex.GetAllStorageNodes()),
			"buckets":      s.storageIndex.GetAllBuckets(),
			"lastUpdated":  s.storageIndex.GetLastRefreshed().Format(time.RFC3339),
		}

		json.NewEncoder(w).Encode(report)
	})

	// Storage summary endpoint
	mux.HandleFunc("/storage-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.storageIndex.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	// Bandwidth summary endpoint
	mux.HandleFunc("/bandwidth-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.bandwidthGraph.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	mux.HandleFunc("/perform-maintenance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		klog.Info("Maintenance requested via API")

		s.storageMutex.Lock()
		start := time.Now()
		s.storageIndex.PerformMaintenance()
		duration := time.Since(start)
		s.storageMutex.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"success","duration":"%v"}`, duration)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		klog.Info("Starting health check server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Health check server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		server.Shutdown(shutdownCtx)
	}()
}

func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func isEdgeNode(node *v1.Node) bool {
	// node label
	if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
		return true
	}

	// node name for edge indicator
	if strings.Contains(strings.ToLower(node.Name), "edge") {
		return true
	}

	return false
}

func hasGPUCapability(node *v1.Node) bool {
	for k, v := range node.Labels {
		if (strings.Contains(k, "gpu") || strings.Contains(k, "accelerator")) && v == "true" {
			return true
		}
	}

	return false
}

func hasFastStorage(node *v1.Node) bool {
	// fast storage labels
	if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
		if tech == "nvme" || tech == "ssd" {
			return true
		}
	}

	// explicit fast storage label
	if val, ok := node.Labels["node-capability/fast-storage"]; ok && val == "true" {
		return true
	}

	return false
}

func podNeedsGPU(pod *v1.Pod) bool {
	if pod.Annotations != nil {
		if _, ok := pod.Annotations["scheduler.thesis/requires-gpu"]; ok {
			return true
		}
	}

	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			for resourceName := range container.Resources.Requests {
				if strings.Contains(string(resourceName), "gpu") ||
					strings.Contains(string(resourceName), "nvidia.com") {
					return true
				}
			}
		}
	}

	return false
}

func podNeedsFastStorage(pod *v1.Pod) bool {
	if pod.Annotations != nil {
		if _, ok := pod.Annotations["scheduler.thesis/requires-fast-storage"]; ok {
			return true
		}
	}

	return false
}

func nodeHasLabelKey(node *v1.Node, key string) bool {
	_, exists := node.Labels[key]
	return exists
}

func nodeHasValueForKey(node *v1.Node, key string, values []string) bool {
	if value, exists := node.Labels[key]; exists {
		for _, v := range values {
			if value == v {
				return true
			}
		}
	}
	return false
}

func nodeMatchesNumericComparison(node *v1.Node, key string, op v1.NodeSelectorOperator, values []string) bool {
	if val, exists := node.Labels[key]; exists && len(values) > 0 {
		nodeVal, err1 := strconv.Atoi(val)
		compareVal, err2 := strconv.Atoi(values[0])

		if err1 == nil && err2 == nil {
			if op == v1.NodeSelectorOpGt {
				return nodeVal > compareVal
			} else if op == v1.NodeSelectorOpLt {
				return nodeVal < compareVal
			}
		}
	}
	return false
}

func extractPodCapabilityRequirements(pod *v1.Pod) map[string]string {
	capabilities := make(map[string]string)

	if pod.Annotations != nil {
		for key, value := range pod.Annotations {
			if strings.HasPrefix(key, "scheduler.thesis/capability-") {
				capName := strings.TrimPrefix(key, "scheduler.thesis/capability-")
				capabilities[capName] = value
			}
		}

		if _, ok := pod.Annotations["scheduler.thesis/requires-gpu"]; ok {
			capabilities["gpu-accelerated"] = "true"
		}

		if _, ok := pod.Annotations["scheduler.thesis/requires-fast-storage"]; ok {
			capabilities["fast-storage"] = "true"
		}
	}

	return capabilities
}

func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for _, toleration := range tolerations {
		if toleration.Effect == taint.Effect &&
			(toleration.Key == taint.Key || toleration.Key == "") &&
			(toleration.Value == taint.Value || toleration.Operator == v1.TolerationOpExists) {
			return true
		}
	}
	return false
}

func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}
