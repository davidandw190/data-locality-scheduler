package daemon

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	"github.com/davidandw190/data-locality-scheduler/pkg/storage/minio"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func (c *DataLocalityCollector) Name() string {
	return "DataLocalityCollector"
}

type DataLocalityCollector struct {
	nodeName              string
	clientset             kubernetes.Interface
	bandwidthCache        map[string]int64
	bandwidthLatencyCache map[string]float64
	bandwidthCacheMutex   sync.RWMutex
	lastBandwidthUpdate   time.Time
	storageCache          map[string]interface{}
	storageCacheMutex     sync.RWMutex
	nodeType              string // edge/cloud
	region                string
	zone                  string
}

func NewDataLocalityCollector(nodeName string, clientset kubernetes.Interface) *DataLocalityCollector {
	return &DataLocalityCollector{
		nodeName:              nodeName,
		clientset:             clientset,
		bandwidthCache:        make(map[string]int64),
		bandwidthLatencyCache: make(map[string]float64),
		storageCache:          make(map[string]interface{}),
		lastBandwidthUpdate:   time.Now().Add(-DefaultBandwidthMeasurementInterval), // we force immediate update
	}
}

func (c *DataLocalityCollector) Collect(ctx context.Context) (map[string]string, error) {
	labels := make(map[string]string)
	startTime := time.Now()

	defer func() {
		klog.V(4).Infof("Data locality collection took %v", time.Since(startTime))
	}()

	node, err := c.clientset.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", c.nodeName, err)
	}

	c.detectAndSetNodeTopology(node, labels)

	storageIndex := storage.NewStorageIndex()
	minioIndexer := minio.NewIndexer(storageIndex, 5*time.Minute)

	detector := NewMinioDetector(c.clientset, c.nodeName, minioIndexer)
	hasMinio, minioBuckets, err := detector.DetectLocalMinioService(ctx)

	if err != nil {
		klog.Warningf("Error detecting Minio: %v", err)
	}

	if hasMinio {
		detector.AddMinioLabelsToNode(labels, minioBuckets)
		klog.V(2).Infof("Detected Minio on node %s with %d buckets", c.nodeName, len(minioBuckets))
	} else {
		volumeDetector := NewVolumeDetector(c.clientset, c.nodeName)
		volumes, err := volumeDetector.DetectLocalVolumes(ctx)

		if err != nil {
			klog.Warningf("Error detecting volumes: %v", err)
		} else if len(volumes) > 0 {
			volumeDetector.AddVolumeLabelsToNode(volumes, labels)
			klog.V(2).Infof("Detected %d volumes on node %s", len(volumes), c.nodeName)
		}
	}

	if time.Since(c.lastBandwidthUpdate) > DefaultBandwidthMeasurementInterval {
		networkCtx, networkCancel := context.WithTimeout(ctx, 2*time.Minute)
		defer networkCancel()

		go func() {
			c.collectNetworkMeasurements(networkCtx, labels)
			c.lastBandwidthUpdate = time.Now()
		}()
	} else {
		c.bandwidthCacheMutex.RLock()
		for nodeName, bandwidth := range c.bandwidthCache {
			labels[BandwidthPrefix+nodeName] = strconv.FormatInt(bandwidth, 10)
		}

		for nodeName, latency := range c.bandwidthLatencyCache {
			labels[LatencyPrefix+nodeName] = strconv.FormatFloat(latency, 'f', 2, 64)
		}
		c.bandwidthCacheMutex.RUnlock()
	}

	return labels, nil
}

func (c *DataLocalityCollector) GetCapabilities() map[string]string {
	c.bandwidthCacheMutex.RLock()
	defer c.bandwidthCacheMutex.RUnlock()

	result := make(map[string]string)
	for nodeName, bandwidth := range c.bandwidthCache {
		result[BandwidthPrefix+nodeName] = strconv.FormatInt(bandwidth, 10)
	}

	for nodeName, latency := range c.bandwidthLatencyCache {
		result[LatencyPrefix+nodeName] = strconv.FormatFloat(latency, 'f', 2, 64)
	}

	return result
}

func (c *DataLocalityCollector) detectAndSetNodeTopology(node *v1.Node, labels map[string]string) {
	if nodeType, exists := node.Labels[EdgeNodeLabel]; exists {
		c.nodeType = nodeType
		labels[EdgeNodeLabel] = nodeType
	} else {
		isEdge := false

		if strings.Contains(strings.ToLower(node.Name), "edge") {
			isEdge = true
		}

		cpuCores := node.Status.Capacity.Cpu().Value()
		memoryBytes := node.Status.Allocatable.Memory().Value()

		if cpuCores <= 4 && memoryBytes <= 8*1024*1024*1024 {
			isEdge = true
		}

		for key, value := range node.Labels {
			if strings.Contains(key, "instance-type") &&
				(strings.Contains(value, "small") || strings.Contains(value, "micro")) {
				isEdge = true
				break
			}
		}

		if isEdge {
			c.nodeType = EdgeNodeValue
			labels[EdgeNodeLabel] = EdgeNodeValue
		} else {
			c.nodeType = CloudNodeValue
			labels[EdgeNodeLabel] = CloudNodeValue
		}
	}

	if region, exists := node.Labels[RegionLabel]; exists {
		c.region = region
		labels[RegionLabel] = region
	}

	if zone, exists := node.Labels[ZoneLabel]; exists {
		c.zone = zone
		labels[ZoneLabel] = zone
	}
}

// collectNetworkMeasurements measures network bandwidth between nodes
func (c *DataLocalityCollector) collectNetworkMeasurements(ctx context.Context, labels map[string]string) {
	klog.Infof("Starting network measurements for node %s", c.nodeName)

	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list nodes for bandwidth measurement: %v", err)
		return
	}

	labels[BandwidthPrefix+"local"] = "1000000000" // 1 GB/s
	labels[LatencyPrefix+"local"] = "0.1"          // 0.1ms

	c.bandwidthCacheMutex.Lock()
	c.bandwidthCache = make(map[string]int64)
	c.bandwidthLatencyCache = make(map[string]float64)
	c.bandwidthCacheMutex.Unlock()

	// we can group nodes by zone and region to prioritize measurements
	var sameZoneNodes, sameRegionNodes, otherNodes []v1.Node

	for _, node := range nodes.Items {
		if node.Name == c.nodeName {
			continue
		}

		if !isNodeReady(&node) {
			continue
		}

		if c.zone != "" && node.Labels[ZoneLabel] == c.zone {
			sameZoneNodes = append(sameZoneNodes, node)
		} else if c.region != "" && node.Labels[RegionLabel] == c.region {
			sameRegionNodes = append(sameRegionNodes, node)
		} else {
			otherNodes = append(otherNodes, node)
		}
	}

	// measurement order: same zone -> same region -> others
	nodesToMeasure := append(sameZoneNodes, sameRegionNodes...)
	nodesToMeasure = append(nodesToMeasure, otherNodes...)

	maxNodesToMeasure := MaxNodesToMeasure
	if len(nodesToMeasure) > maxNodesToMeasure {
		nodesToMeasure = nodesToMeasure[:maxNodesToMeasure]
	}

	measuredCount := 0
	for _, node := range nodesToMeasure {
		var nodeIP string
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeInternalIP {
				nodeIP = address.Address
				break
			}
		}

		if nodeIP == "" {
			klog.V(4).Infof("Skipping bandwidth measurement to node %s (no internal IP)", node.Name)
			continue
		}

		bandwidth, latency := c.mockBandwidthMeasurement(node)

		labels[BandwidthPrefix+node.Name] = strconv.FormatInt(bandwidth, 10)
		labels[LatencyPrefix+node.Name] = strconv.FormatFloat(latency, 'f', 2, 64)

		c.bandwidthCacheMutex.Lock()
		c.bandwidthCache[node.Name] = bandwidth
		c.bandwidthLatencyCache[node.Name] = latency
		c.bandwidthCacheMutex.Unlock()

		measuredCount++
		klog.V(4).Infof("Measured bandwidth to %s: %d bytes/sec, %.2f ms", node.Name, bandwidth, latency)
	}

	klog.Infof("Network measurements complete for node %s: measured %d nodes", c.nodeName, measuredCount)
}

// TEMP: mockBandwidthMeasurement creates realistic bandwidth/latency measurements based on topology
func (c *DataLocalityCollector) mockBandwidthMeasurement(node v1.Node) (int64, float64) {
	targetType := CloudNodeValue
	if value, exists := node.Labels[EdgeNodeLabel]; exists && value == EdgeNodeValue {
		targetType = EdgeNodeValue
	} else if strings.Contains(strings.ToLower(node.Name), "edge") {
		targetType = EdgeNodeValue
	}

	targetRegion := node.Labels[RegionLabel]
	targetZone := node.Labels[ZoneLabel]

	var bandwidth int64
	var latency float64

	jitter := 0.85 + (rand.Float64() * 0.3)

	// all these are estimations based on topology relationship
	if c.zone != "" && c.zone == targetZone {
		// same zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(500000000) * jitter) // 500 MB/s edge-edge
			latency = 1.0 / jitter                         // 1ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(1000000000) * jitter) // 1 GB/s cloud-cloud
			latency = 0.5 / jitter                          // 0.5ms
		} else {
			bandwidth = int64(float64(750000000) * jitter) // 750 MB/s edge-cloud
			latency = 1.0 / jitter                         // 1ms
		}
	} else if c.region != "" && c.region == targetRegion {
		// same region, different zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(200000000) * jitter) // 200 MB/s edge-edge
			latency = 5.0 / jitter                         // 5ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(500000000) * jitter) // 500 MB/s cloud-cloud
			latency = 2.0 / jitter                         // 2ms
		} else {
			bandwidth = int64(float64(100000000) * jitter) // 100 MB/s edge-cloud
			latency = 10.0 / jitter                        // 10ms
		}
	} else {
		// different regions
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(50000000) * jitter) // 50 MB/s edge-edge
			latency = 50.0 / jitter                       // 50ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(100000000) * jitter) // 100 MB/s cloud-cloud
			latency = 20.0 / jitter                        // 20ms
		} else {
			bandwidth = int64(float64(20000000) * jitter) // 20 MB/s edge-cloud
			latency = 100.0 / jitter                      // 100ms
		}
	}

	return bandwidth, latency
}

func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
