package scheduler

import (
	"context"
	"encoding/json"
	"flag"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	nodeDataLocalityScore  *prometheus.GaugeVec
	podPlacementByNodeType *prometheus.CounterVec
	dataTransferTime       *prometheus.HistogramVec
	dataTransferBytes      *prometheus.CounterVec
	bandwidthUtilization   *prometheus.GaugeVec
	storageAccessLatency   *prometheus.HistogramVec
	edgeUtilizationRatio   *prometheus.GaugeVec

	resourceEfficiency *prometheus.GaugeVec
	podStartupLatency  *prometheus.HistogramVec
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
	config               *SchedulerConfig
	metricsCollector     *SchedulerMetricsCollector
}

type PriorityFunc func(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error)

func NewScheduler(clientset kubernetes.Interface, config *SchedulerConfig) *Scheduler {
	if config == nil {
		config = NewDefaultConfig()
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: clientset.CoreV1().Events(""),
		},
	)

	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		v1.EventSource{Component: config.SchedulerName},
	)

	if config.VerboseLogging {
		flag.Set("v", "4")
	}

	scheduler := &Scheduler{
		clientset:         clientset,
		schedulerName:     config.SchedulerName,
		podQueue:          make(chan *v1.Pod, config.PodQueueSize),
		storageIndex:      storage.NewStorageIndex(),
		bandwidthGraph:    storage.NewBandwidthGraph(config.SameRegionBandwidth), // Default bandwidth
		priorityFuncs:     make([]PriorityFunc, 0),
		nodeResourceCache: make(map[string]*nodeResources),
		config:            config,
		enableMockData:    config.EnableMockData,
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
			nodeDataLocalityScore: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "scheduler_node_data_locality_score",
				Help: "Data locality score for each node (0-100)",
			}, []string{"node", "node_type", "region", "zone"}),

			podPlacementByNodeType: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "scheduler_pod_placement_total",
				Help: "Count of pods placed on different node types",
			}, []string{"node_type", "region", "zone", "pod_namespace"}),

			dataTransferTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "scheduler_data_transfer_time_seconds",
				Help:    "Estimated time to transfer data for scheduled workloads",
				Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // From 10ms to ~20s
			}, []string{"source_node", "destination_node", "data_type"}),

			dataTransferBytes: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "scheduler_data_transfer_bytes_total",
				Help: "Amount of data transferred between nodes due to scheduling decisions",
			}, []string{"source_node", "destination_node", "data_type"}),

			bandwidthUtilization: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "scheduler_bandwidth_utilization_bytes_per_second",
				Help: "Bandwidth utilization between nodes based on scheduling decisions",
			}, []string{"source_node", "destination_node"}),

			storageAccessLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "scheduler_storage_access_latency_seconds",
				Help:    "Latency of storage access operations based on scheduling decisions",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
			}, []string{"node", "storage_type"}),

			edgeUtilizationRatio: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "scheduler_edge_utilization_ratio",
				Help: "Ratio of pods scheduled on edge nodes (0.0-1.0)",
			}, []string{"region", "zone"}),

			resourceEfficiency: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "scheduler_resource_efficiency",
				Help: "Resource efficiency score based on pod-node fit (0-100)",
			}, []string{"node", "resource_type"}),

			podStartupLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "scheduler_pod_startup_latency_seconds",
				Help:    "Time from scheduling to pod running state",
				Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // From 100ms to ~51s
			}, []string{"node_type", "pod_type"}),
		},
		metricsCollector: NewSchedulerMetricsCollector(),
		recorder:         recorder,
		retryCount:       make(map[string]int),
	}

	scheduler.bandwidthGraph.SetTopologyDefaults(
		config.LocalBandwidth, config.LocalLatency,
		config.SameZoneBandwidth, config.SameZoneLatency,
		config.SameRegionBandwidth, config.SameRegionLatency,
		config.EdgeCloudBandwidth, config.EdgeCloudLatency,
	)

	dataLocalityConfig := &DataLocalityConfig{
		InputDataWeight:      0.7, // 70% weight for input data
		OutputDataWeight:     0.3, // 30% weight for output data
		DataTransferWeight:   0.8, // 80% weight for data transfer time
		MaxScore:             config.MaxResourceScore,
		DefaultScore:         config.DefaultScore,
		LocalBandwidth:       config.LocalBandwidth,
		SameZoneBandwidth:    config.SameZoneBandwidth,
		SameRegionBandwidth:  config.SameRegionBandwidth,
		CrossRegionBandwidth: config.CrossRegionBandwidth,
	}

	scheduler.dataLocalityPriority = NewDataLocalityPriority(
		scheduler.storageIndex,
		scheduler.bandwidthGraph,
		dataLocalityConfig,
	)

	scheduler.initPriorityFunctions()

	return scheduler
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
	}

	if s.config.ResourceCacheEnabled {
		go s.startCacheCleanup(ctx)
	}

	if s.config.EnableMockData {
		klog.Info("Creating mock storage data for testing")
		s.storageIndex.MockMinioData()
		s.bandwidthGraph.MockNetworkPaths()
	}

	go s.refreshStorageDataPeriodically(ctx)

	go s.startHealthCheckServer(ctx)

	go wait.UntilWithContext(ctx, s.scheduleOne, s.config.SchedulerInterval)

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
	ticker := time.NewTicker(s.config.RefreshInterval)
	defer ticker.Stop()

	maintenanceTicker := time.NewTicker(s.config.RefreshInterval * 4)
	defer maintenanceTicker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.refreshStorageInformation(ctx); err != nil {
				klog.Warningf("Failed to refresh storage information: %v", err)
			}
		case <-maintenanceTicker.C:
			s.storageMutex.Lock()
			s.storageIndex.PerformMaintenance()
			s.storageMutex.Unlock()
			klog.V(3).Info("Scheduled storage maintenance completed")
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

		namespaces := []string{
			"data-locality-scheduler",
			"scheduler-benchmark",
		}

		servicePatterns := []struct {
			namePrefix string
			domains    []string
			ports      []int
		}{
			{
				namePrefix: "minio",
				domains:    []string{".svc.cluster.local"},
				ports:      []int{9000},
			},
			{
				namePrefix: "minio-central",
				domains:    []string{".svc.cluster.local"},
				ports:      []int{9000},
			},
			{
				namePrefix: "minio-edge-region1",
				domains:    []string{".svc.cluster.local"},
				ports:      []int{9000},
			},
			{
				namePrefix: "minio-edge-region2",
				domains:    []string{".svc.cluster.local"},
				ports:      []int{9000},
			},
		}

		for _, ns := range namespaces {
			for _, svcPattern := range servicePatterns {
				for _, domain := range svcPattern.domains {
					for _, port := range svcPattern.ports {
						var serviceName, endpoint string

						if ns == "" {
							serviceName = svcPattern.namePrefix
							endpoint = fmt.Sprintf("%s%s:%d", svcPattern.namePrefix, domain, port)
						} else {
							serviceName = fmt.Sprintf("%s-%s", svcPattern.namePrefix, ns)
							endpoint = fmt.Sprintf("%s.%s%s:%d", svcPattern.namePrefix, ns, domain, port)
						}

						klog.V(4).Infof("Attempting to register MinIO service %s at %s", serviceName, endpoint)
						minioIndexer.RegisterMinioService(serviceName, endpoint, false)
					}
				}
			}
		}

		// we try direct IP-based discovery as a last resort
		if err := s.discoverMinioByPodIP(ctx, minioIndexer); err != nil {
			klog.Warningf("Pod IP-based MinIO discovery failed: %v", err)
		}

		if err := minioIndexer.RefreshIndex(ctx); err != nil {
			klog.Warningf("Failed to refresh MinIO index in second attempt: %v", err)
		}

		s.storageMutex.RLock()
		dataItemCount = len(s.storageIndex.GetAllDataItems())
		s.storageMutex.RUnlock()

		klog.Infof("After aggressive discovery: found %d data items", dataItemCount)

		if dataItemCount == 0 && s.enableMockData {
			klog.Warning("No MinIO data items found after exhaustive discovery. Creating mock data.")
			s.storageIndex.MockMinioData()
			s.bandwidthGraph.MockNetworkPaths()
		}
	}

	minioIndexer.StartRefresher(ctx)
	return nil
}

func (s *Scheduler) discoverMinioByPodIP(ctx context.Context, indexer *minio.Indexer) error {
	namespaces := []string{"data-locality-scheduler", "scheduler-benchmark", ""}

	var podList *v1.PodList
	var err error

	for _, ns := range namespaces {
		labelSelectors := []string{
			"app=minio",
			"app in (minio,minio-edge,minio-central)",
			"role in (central,edge)",
		}

		for _, selector := range labelSelectors {
			options := metav1.ListOptions{
				LabelSelector: selector,
			}

			if ns == "" {
				podList, err = s.clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, options)
			} else {
				podList, err = s.clientset.CoreV1().Pods(ns).List(ctx, options)
			}

			if err != nil {
				klog.Warningf("Failed to list pods with selector %s in namespace %s: %v",
					selector, ns, err)
				continue
			}

			if len(podList.Items) == 0 {
				continue
			}

			klog.Infof("Found %d potential MinIO pods with selector %s in namespace %s",
				len(podList.Items), selector, ns)

			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
					continue
				}

				var port int32 = 9000
				for _, container := range pod.Spec.Containers {
					for _, containerPort := range container.Ports {
						if containerPort.ContainerPort == 9000 {
							port = containerPort.ContainerPort
							break
						}
					}
				}

				endpoint := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
				serviceName := fmt.Sprintf("%s-%s", pod.Name, pod.Namespace)

				klog.Infof("Registering MinIO pod %s/%s with IP %s",
					pod.Namespace, pod.Name, endpoint)
				indexer.RegisterMinioService(serviceName, endpoint, false)

				if pod.Spec.NodeName != "" {
					klog.Infof("Registering pod's node %s as storage endpoint", pod.Spec.NodeName)
					indexer.RegisterMinioNode(pod.Spec.NodeName, endpoint, false)
				}
			}
		}
	}

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
	klog.Infof("Finding best node for pod %s/%s", pod.Namespace, pod.Name)

	startTime := time.Now()

	if pod.Annotations != nil {
		dataAnnotations := []string{}
		for k := range pod.Annotations {
			if strings.HasPrefix(k, "data.scheduler.thesis/") {
				dataAnnotations = append(dataAnnotations, k)
			}
		}

		if len(dataAnnotations) > 0 {
			klog.Infof("Pod has %d data annotations: %v", len(dataAnnotations), dataAnnotations)
		}

		if val, ok := pod.Annotations["scheduler.thesis/data-intensive"]; ok {
			klog.Infof("Pod is marked as data-intensive with value: %s", val)
		}
	}

	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no nodes available in the cluster")
	}

	klog.V(4).Infof("Available nodes for scheduling:")
	for _, node := range nodes.Items {
		nodeType := "standard"
		if val, ok := node.Labels[EdgeNodeLabel]; ok {
			nodeType = val
		}

		isStorage := "no"
		if val, ok := node.Labels[StorageNodeLabel]; ok && val == "true" {
			isStorage = "yes"
		}

		region := node.Labels[RegionLabel]
		zone := node.Labels[ZoneLabel]

		klog.V(4).Infof("- Node: %s (type: %s, storage: %s, region: %s, zone: %s)",
			node.Name, nodeType, isStorage, region, zone)
	}

	filteredNodes, err := s.filterNodes(ctx, pod, nodes.Items)
	if err != nil {
		return "", fmt.Errorf("node filtering error: %w", err)
	}

	klog.Infof("Filtered to %d suitable nodes for pod %s/%s",
		len(filteredNodes), pod.Namespace, pod.Name)

	nodesToScore := filteredNodes
	if len(filteredNodes) > 1 && s.config.PercentageOfNodesToScore < 100 {
		numNodes := len(filteredNodes) * s.config.PercentageOfNodesToScore / 100
		if numNodes < 1 {
			numNodes = 1
		}
		if numNodes > s.config.MinFeasibleNodesToFind {
			numNodes = s.config.MinFeasibleNodesToFind
		}

		if numNodes < len(filteredNodes) {
			nodesToScore = filteredNodes[:numNodes]
			klog.V(4).Infof("Limiting scoring to %d nodes (%.0f%% of %d filtered nodes)",
				numNodes, float64(s.config.PercentageOfNodesToScore), len(filteredNodes))
		}
	}

	nodeScores, err := s.prioritizeNodes(pod, nodesToScore)
	if err != nil {
		return "", fmt.Errorf("node prioritization error: %w", err)
	}

	if len(nodeScores) == 0 {
		return "", fmt.Errorf("no suitable nodes found after scoring")
	}

	sort.Slice(nodeScores, func(i, j int) bool {
		return nodeScores[i].Score > nodeScores[j].Score
	})

	klog.Infof("Node scores for pod %s/%s:", pod.Namespace, pod.Name)
	for i, score := range nodeScores {
		klog.Infof("  %d. Node: %s, Score: %d", i+1, score.Name, score.Score)
	}

	selectedNode := nodeScores[0].Name

	if selectedNode != "" {
		s.metrics.schedulingLatency.Observe(time.Since(startTime).Seconds())

		// Get node type, region, zone
		nodeObj, err := s.clientset.CoreV1().Nodes().Get(ctx, selectedNode, metav1.GetOptions{})
		if err == nil {
			nodeType := "unknown"
			region := "unknown"
			zone := "unknown"

			if val, ok := nodeObj.Labels["node-capability/node-type"]; ok {
				nodeType = val
			}
			if val, ok := nodeObj.Labels["topology.kubernetes.io/region"]; ok {
				region = val
			}
			if val, ok := nodeObj.Labels["topology.kubernetes.io/zone"]; ok {
				zone = val
			}

			// Record pod placement by node type
			s.metrics.podPlacementByNodeType.WithLabelValues(
				nodeType, region, zone, pod.Namespace,
			).Inc()
		}

		// Track data locality scores
		s.recordDataLocalityMetrics(pod, selectedNode)
	}

	klog.Infof("Selected node %s with highest score %d for pod %s/%s",
		selectedNode, nodeScores[0].Score, pod.Namespace, pod.Name)

	return selectedNode, nil
}

func (s *Scheduler) recordDataLocalityMetrics(pod *v1.Pod, nodeName string) {
	// Get node details
	nodeObj, err := s.clientset.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s for metrics: %v", nodeName, err)
		return
	}

	nodeType := "unknown"
	region := "unknown"
	zone := "unknown"

	if val, ok := nodeObj.Labels["node-capability/node-type"]; ok {
		nodeType = val
	}
	if val, ok := nodeObj.Labels["topology.kubernetes.io/region"]; ok {
		region = val
	}
	if val, ok := nodeObj.Labels["topology.kubernetes.io/zone"]; ok {
		zone = val
	}

	// Record the node data locality score (from your existing dataLocalityPriority calculation)
	// This assumes your scheduler already calculates this value
	if s.dataLocalityPriority != nil {
		score, err := s.dataLocalityPriority.Score(pod, nodeName)
		if err == nil {
			s.metrics.nodeDataLocalityScore.WithLabelValues(
				nodeName, nodeType, region, zone,
			).Set(float64(score))
		}
	}

	// Calculate and record edge utilization ratio
	s.updateEdgeUtilizationRatio(region, zone)

	inputData, outputData, err := s.extractDataDependencies(pod)
	if err == nil {
		s.recordDataTransferMetrics(pod, nodeName, inputData, outputData)
	}

	// Record resource efficiency
	s.recordResourceEfficiencyMetrics(pod, nodeName)
}

// Extract data dependencies from pod (reuse your existing logic if available)
func (s *Scheduler) extractDataDependencies(pod *v1.Pod) ([]DataDependency, []DataDependency, error) {
	var inputData []DataDependency
	var outputData []DataDependency
	var parseErrors []string

	if pod.Annotations == nil {
		return inputData, outputData, nil
	}

	// input data dependencies
	for k, v := range pod.Annotations {
		if strings.HasPrefix(k, "data.scheduler.thesis/input-") {
			// format: urn,size_bytes[,processing_time[,priority[,data_type]]]
			parts := strings.Split(v, ",")
			if len(parts) < 2 {
				parseErrors = append(parseErrors,
					fmt.Sprintf("invalid format for %s: %s (need at least URN,size)", k, v))
				continue
			}

			urn := strings.TrimSpace(parts[0])
			if urn == "" {
				parseErrors = append(parseErrors, fmt.Sprintf("empty URN in %s", k))
				continue
			}

			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				parseErrors = append(parseErrors, fmt.Sprintf("invalid size in %s: %s", k, parts[1]))
				size = 1024 * 1024 // 1MB default
			}

			processingTime := 0
			priority := 5 // default priority
			dataType := "generic"

			if len(parts) > 2 {
				if pt, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
					processingTime = pt
				}
			}

			if len(parts) > 3 {
				if p, err := strconv.Atoi(strings.TrimSpace(parts[3])); err == nil {
					priority = p
				}
			}

			if len(parts) > 4 {
				dataType = strings.TrimSpace(parts[4])
			}

			weight := float64(priority) * math.Log1p(float64(size)/float64(1024*1024))
			if weight < 1.0 {
				weight = 1.0
			}

			inputData = append(inputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: processingTime,
				Priority:       priority,
				DataType:       dataType,
				Weight:         weight,
			})
		} else if strings.HasPrefix(k, "data.scheduler.thesis/output-") {
			// format: urn,size_bytes[,processing_time[,priority[,data_type]]]
			parts := strings.Split(v, ",")
			if len(parts) < 2 {
				parseErrors = append(parseErrors,
					fmt.Sprintf("invalid format for %s: %s (need at least URN,size)", k, v))
				continue
			}

			urn := strings.TrimSpace(parts[0])
			if urn == "" {
				parseErrors = append(parseErrors, fmt.Sprintf("empty URN in %s", k))
				continue
			}

			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				parseErrors = append(parseErrors, fmt.Sprintf("invalid size in %s: %s", k, parts[1]))
				size = 1024 * 1024 // 1MB default
			}

			processingTime := 0
			priority := 5 // default priority
			dataType := "generic"

			if len(parts) > 2 {
				if pt, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
					processingTime = pt
				}
			}

			if len(parts) > 3 {
				if p, err := strconv.Atoi(strings.TrimSpace(parts[3])); err == nil {
					priority = p
				}
			}

			if len(parts) > 4 {
				dataType = strings.TrimSpace(parts[4])
			}

			weight := float64(priority) * math.Log1p(float64(size)/float64(1024*1024))
			if weight < 1.0 {
				weight = 1.0
			}

			outputData = append(outputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: processingTime,
				Priority:       priority,
				DataType:       dataType,
				Weight:         weight,
			})
		}
	}

	if eoInput, ok := pod.Annotations["data.scheduler.thesis/eo-input"]; ok {
		parts := strings.Split(eoInput, ",")
		if len(parts) >= 2 {
			urn := strings.TrimSpace(parts[0])
			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				size = 100 * 1024 * 1024 // 100MB default
			}

			weight := 8.0 * math.Log1p(float64(size)/float64(1024*1024))

			inputData = append(inputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: 30,
				Priority:       8,
				DataType:       "eo-imagery",
				Weight:         weight,
			})
		}
	}

	if eoOutput, ok := pod.Annotations["data.scheduler.thesis/eo-output"]; ok {
		parts := strings.Split(eoOutput, ",")
		if len(parts) >= 2 {
			urn := strings.TrimSpace(parts[0])
			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				size = 50 * 1024 * 1024 // 50MB default
			}

			weight := 7.0 * math.Log1p(float64(size)/float64(1024*1024))

			outputData = append(outputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: 0,
				Priority:       7,
				DataType:       "cog",
				Weight:         weight,
			})
		}
	}

	if len(parseErrors) > 0 {
		return inputData, outputData, fmt.Errorf("data dependency parsing errors: %s",
			strings.Join(parseErrors, "; "))
	}

	return inputData, outputData, nil
}

// Record data transfer metrics based on the pod's data dependencies
func (s *Scheduler) recordDataTransferMetrics(pod *v1.Pod, nodeName string, inputData, outputData []DataDependency) {
	// For each input dependency, calculate transfer time and size
	for _, data := range inputData {
		// Find storage nodes for this data
		storageNodes := s.storageIndex.GetStorageNodesForData(data.URN)
		if len(storageNodes) == 0 {
			// Try to get bucket nodes
			parts := strings.SplitN(data.URN, "/", 2)
			if len(parts) > 0 {
				bucket := parts[0]
				storageNodes = s.storageIndex.GetBucketNodes(bucket)
			}
		}

		// Skip if this data is already on target node
		if containsString(storageNodes, nodeName) {
			continue
		}

		// Find best source node
		var bestSourceNode string
		bestTransferTime := math.MaxFloat64

		for _, sourceNode := range storageNodes {
			transferTime := s.bandwidthGraph.EstimateTransferTimeBetweenNodes(
				sourceNode, nodeName, data.SizeBytes)
			if transferTime < bestTransferTime {
				bestTransferTime = transferTime
				bestSourceNode = sourceNode
			}
		}

		if bestSourceNode != "" {
			// Record transfer metrics
			dataType := "unknown"
			if strings.Contains(data.URN, "eo-scenes") {
				dataType = "eo-imagery"
			} else if strings.Contains(data.URN, "fmask") {
				dataType = "mask"
			} else if strings.Contains(data.URN, "cog") {
				dataType = "cog"
			}

			s.metrics.dataTransferTime.WithLabelValues(
				bestSourceNode, nodeName, dataType,
			).Observe(bestTransferTime)

			s.metrics.dataTransferBytes.WithLabelValues(
				bestSourceNode, nodeName, dataType,
			).Add(float64(data.SizeBytes))

			// Calculate and record bandwidth utilization
			// bandwidth := s.bandwidthGraph.GetBandwidth(bestSourceNode, nodeName)
			s.metrics.bandwidthUtilization.WithLabelValues(
				bestSourceNode, nodeName,
			).Set(float64(data.SizeBytes) / bestTransferTime)
		}
	}

	// Similar logic for output data
	for _, data := range outputData {
		parts := strings.SplitN(data.URN, "/", 2)
		if len(parts) == 0 {
			continue
		}

		bucket := parts[0]
		storageNodes := s.storageIndex.GetBucketNodes(bucket)

		// Skip if this node is a storage node for the bucket
		if containsString(storageNodes, nodeName) {
			continue
		}

		// Find best destination node
		var bestDestNode string
		bestTransferTime := math.MaxFloat64

		for _, destNode := range storageNodes {
			transferTime := s.bandwidthGraph.EstimateTransferTimeBetweenNodes(
				nodeName, destNode, data.SizeBytes)
			if transferTime < bestTransferTime {
				bestTransferTime = transferTime
				bestDestNode = destNode
			}
		}

		if bestDestNode != "" {
			// Record transfer metrics
			dataType := "unknown"
			if strings.Contains(data.URN, "eo-scenes") {
				dataType = "eo-imagery"
			} else if strings.Contains(data.URN, "fmask") {
				dataType = "mask"
			} else if strings.Contains(data.URN, "cog") {
				dataType = "cog"
			}

			s.metrics.dataTransferTime.WithLabelValues(
				nodeName, bestDestNode, dataType,
			).Observe(bestTransferTime)

			s.metrics.dataTransferBytes.WithLabelValues(
				nodeName, bestDestNode, dataType,
			).Add(float64(data.SizeBytes))

			// Calculate and record bandwidth utilization
			// bandwidth := s.bandwidthGraph.GetBandwidth(nodeName, bestDestNode)
			s.metrics.bandwidthUtilization.WithLabelValues(
				nodeName, bestDestNode,
			).Set(float64(data.SizeBytes) / bestTransferTime)
		}
	}
}

// Update the edge utilization ratio metric
func (s *Scheduler) updateEdgeUtilizationRatio(region, zone string) {
	// Get all nodes
	nodes, err := s.clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return
	}

	// Get all pods
	pods, err := s.clientset.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return
	}

	// Count pods on each node type
	edgeCount := 0
	totalCount := 0

	podsByNode := make(map[string]int)
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != "" {
			podsByNode[pod.Spec.NodeName]++
			totalCount++
		}
	}

	for _, node := range nodes.Items {
		if value, ok := node.Labels["node-capability/node-type"]; ok && value == "edge" {
			edgeCount += podsByNode[node.Name]
		}
	}

	// Calculate and record ratio
	if totalCount > 0 {
		ratio := float64(edgeCount) / float64(totalCount)
		s.metrics.edgeUtilizationRatio.WithLabelValues(region, zone).Set(ratio)
	}
}

// Record resource efficiency metrics
func (s *Scheduler) recordResourceEfficiencyMetrics(pod *v1.Pod, nodeName string) {
	node, err := s.clientset.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return
	}

	// Calculate CPU efficiency
	var requestedCPU int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // Default 100m
		}
	}

	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	if allocatableCPU > 0 {
		cpuEfficiency := 100 - (float64(requestedCPU) / float64(allocatableCPU) * 100)
		s.metrics.resourceEfficiency.WithLabelValues(nodeName, "cpu").Set(cpuEfficiency)
	}

	// Calculate memory efficiency
	var requestedMemory int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // Default 200Mi
		}
	}

	allocatableMemory := node.Status.Allocatable.Memory().Value()
	if allocatableMemory > 0 {
		memoryEfficiency := 100 - (float64(requestedMemory) / float64(allocatableMemory) * 100)
		s.metrics.resourceEfficiency.WithLabelValues(nodeName, "memory").Set(memoryEfficiency)
	}
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
		s.config.ResourceWeight,
		s.config.NodeAffinityWeight,
		s.config.NodeTypeWeight,
		s.config.CapabilitiesWeight,
		s.config.DataLocalityWeight,
	}

	if pod.Annotations == nil {
		return weights
	}

	if _, ok := pod.Annotations[AnnotationDataIntensive]; ok {
		weights = []float64{
			s.config.DataIntensiveResourceWeight,
			s.config.DataIntensiveNodeAffinityWeight,
			s.config.DataIntensiveNodeTypeWeight,
			s.config.DataIntensiveCapabilitiesWeight,
			s.config.DataIntensiveDataLocalityWeight,
		}
	} else if _, ok := pod.Annotations[AnnotationComputeIntensive]; ok {
		weights = []float64{
			s.config.ComputeIntensiveResourceWeight,
			s.config.ComputeIntensiveNodeAffinityWeight,
			s.config.ComputeIntensiveNodeTypeWeight,
			s.config.ComputeIntensiveCapabilitiesWeight,
			s.config.ComputeIntensiveDataLocalityWeight,
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

// startCacheCleanup starts a background routine to clean up expired cache entries
func (s *Scheduler) startCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(s.config.ResourceCacheExpiration / 2)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				s.cleanupCache()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *Scheduler) cleanupCache() {
	start := time.Now()

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	expiredKeys := 0
	for key, resources := range s.nodeResourceCache {
		if time.Since(resources.timestamp) > s.config.ResourceCacheExpiration {
			delete(s.nodeResourceCache, key)
			expiredKeys++
		}
	}

	duration := time.Since(start)
	klog.V(4).Infof("Cache cleanup: removed %d expired entries in %v", expiredKeys, duration)
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

	// Calculate data locality score and record metrics before binding
	inputData, outputData, _ := s.extractDataDependencies(pod)
	dataLocalityScore := 50 // Default score
	if s.dataLocalityPriority != nil {
		score, err := s.dataLocalityPriority.Score(pod, nodeName)
		if err == nil {
			dataLocalityScore = score
		}
	}

	// Record detailed scheduling metrics
	s.recordSchedulingMetrics(pod, nodeName, dataLocalityScore, inputData, outputData)

	return s.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
}

func (s *Scheduler) startHealthCheckServer(ctx context.Context) {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Readiness check
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		nodes := s.storageIndex.GetAllStorageNodes()
		if len(nodes) > 0 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Ready"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready - awaiting node information"))
		}
	})

	// Storage info
	mux.HandleFunc("/storage-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		s.storageMutex.RLock()
		defer s.storageMutex.RUnlock()

		nodes := s.storageIndex.GetAllStorageNodes()
		buckets := s.storageIndex.GetAllBuckets()

		edgeCount := 0
		cloudCount := 0
		for _, node := range nodes {
			if node.NodeType == storage.StorageTypeEdge {
				edgeCount++
			} else {
				cloudCount++
			}
		}

		regions := make(map[string]int)
		zones := make(map[string]int)
		for _, node := range nodes {
			if node.Region != "" {
				regions[node.Region]++
			}
			if node.Zone != "" {
				zones[node.Zone]++
			}
		}

		dataItems := s.storageIndex.GetAllDataItems()

		report := map[string]interface{}{
			"storageNodes":  len(nodes),
			"edgeNodes":     edgeCount,
			"cloudNodes":    cloudCount,
			"buckets":       buckets,
			"bucketCount":   len(buckets),
			"dataItemCount": len(dataItems),
			"regions":       regions,
			"zones":         zones,
			"lastUpdated":   s.storageIndex.GetLastRefreshed().Format(time.RFC3339),
		}

		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/storage-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.storageIndex.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	// Bandwidth summary
	mux.HandleFunc("/bandwidth-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.bandwidthGraph.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	// Scheduler stats
	mux.HandleFunc("/scheduler-stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		stats := map[string]interface{}{
			"queueLength": len(s.podQueue),
			"cacheSize":   len(s.nodeResourceCache),
		}

		json.NewEncoder(w).Encode(stats)
	})

	// Data distribution
	mux.HandleFunc("/data-distribution", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		s.storageMutex.RLock()
		defer s.storageMutex.RUnlock()

		allDataItems := s.storageIndex.GetAllDataItems()
		storageNodes := s.storageIndex.GetAllStorageNodes()

		nodeTypes := make(map[string]string)
		nodeRegions := make(map[string]string)
		nodeZones := make(map[string]string)

		for _, node := range storageNodes {
			nodeTypes[node.Name] = string(node.NodeType)
			nodeRegions[node.Name] = node.Region
			nodeZones[node.Name] = node.Zone
		}

		dataDetails := make(map[string]map[string]interface{})

		buckets := make(map[string]int)

		for urn, item := range allDataItems {
			parts := strings.SplitN(urn, "/", 2)
			bucket := ""
			if len(parts) > 0 {
				bucket = parts[0]
				buckets[bucket]++
			}

			locationInfo := make([]map[string]string, 0, len(item.Locations))
			for _, nodeName := range item.Locations {
				locationInfo = append(locationInfo, map[string]string{
					"node":   nodeName,
					"type":   nodeTypes[nodeName],
					"region": nodeRegions[nodeName],
					"zone":   nodeZones[nodeName],
				})
			}

			dataDetails[urn] = map[string]interface{}{
				"size":         item.Size,
				"contentType":  item.ContentType,
				"bucket":       bucket,
				"lastModified": item.LastModified.Format(time.RFC3339),
				"nodeCount":    len(item.Locations),
				"locations":    locationInfo,
			}

			if len(item.Metadata) > 0 {
				dataDetails[urn]["metadata"] = item.Metadata
			}
		}

		stats := map[string]interface{}{
			"totalDataItems": len(allDataItems),
			"totalNodes":     len(storageNodes),
			"bucketCounts":   buckets,
			"timestamp":      time.Now().Format(time.RFC3339),
		}

		response := map[string]interface{}{
			"summary": stats,
			"data":    dataDetails,
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(response); err != nil {
			klog.Errorf("Error encoding data distribution response: %v", err)
			http.Error(w, "Error generating response", http.StatusInternalServerError)
		}
	})

	// Perform maintenance
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

	if s.metricsCollector != nil {
		mux.HandleFunc("/data-locality-stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats := s.metricsCollector.GetDataLocalityStats()
			if err := json.NewEncoder(w).Encode(stats); err != nil {
				klog.Errorf("Failed to encode data locality stats: %v", err)
				http.Error(w, "Error encoding response", http.StatusInternalServerError)
			}
		})

		mux.HandleFunc("/recent-scheduling-decisions", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			decisions := s.metricsCollector.GetRecentSchedulingDecisions()
			if err := json.NewEncoder(w).Encode(decisions); err != nil {
				klog.Errorf("Failed to encode scheduling decisions: %v", err)
				http.Error(w, "Error encoding response", http.StatusInternalServerError)
			}
		})
	}

	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.HealthServerPort),
		Handler: mux,
	}

	go func() {
		klog.Infof("Starting health check server on port %d", s.config.HealthServerPort)
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
