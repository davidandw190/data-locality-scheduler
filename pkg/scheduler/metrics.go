package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

type SchedulerMetricsCollector struct {
	mu                       sync.RWMutex
	schedulingDecisions      []SchedulingDecision
	podDataLocalityStats     map[string]PodDataLocalityStats // pod UID -> stats
	nodeDataLocalityScore    map[string]float64              // node name -> score
	regionTransferStatistics map[string]RegionTransferStats  // region -> stats
}

type SchedulingDecision struct {
	PodName           string    `json:"podName"`
	PodNamespace      string    `json:"podNamespace"`
	PodUID            string    `json:"podUID"`
	NodeName          string    `json:"nodeName"`
	NodeType          string    `json:"nodeType"`
	NodeRegion        string    `json:"nodeRegion"`
	NodeZone          string    `json:"nodeZone"`
	Timestamp         time.Time `json:"timestamp"`
	DataLocalityScore int       `json:"dataLocalityScore"`

	// Data locality information
	DataItems            []DataItemLocation  `json:"dataItems"`
	LocalDataItems       int                 `json:"localDataItems"`
	SameRegionDataItems  int                 `json:"sameRegionDataItems"`
	CrossRegionDataItems int                 `json:"crossRegionDataItems"`
	TotalDataItemSize    int64               `json:"totalDataItemSize"`
	LocalDataSize        int64               `json:"localDataSize"`
	SameRegionDataSize   int64               `json:"sameRegionDataSize"`
	CrossRegionDataSize  int64               `json:"crossRegionDataSize"`
	DataTransferSummary  DataTransferSummary `json:"dataTransferSummary"`
	DecisionFactors      map[string]float64  `json:"decisionFactors"`
}

type DataItemLocation struct {
	URN          string   `json:"urn"`
	Size         int64    `json:"size"`
	IsLocal      bool     `json:"isLocal"`
	IsSameRegion bool     `json:"isSameRegion"`
	Locations    []string `json:"locations"`
}

type DataTransferSummary struct {
	EstimatedTransferTime   float64 `json:"estimatedTransferTime"`
	EstimatedBandwidthUsage float64 `json:"estimatedBandwidthUsage"`
	DataLocalityPercentage  float64 `json:"dataLocalityPercentage"` // % of data items local to the pod
	LocalAccessPercentage   float64 `json:"localAccessPercentage"`  // % of data size accessible locally
}

type PodDataLocalityStats struct {
	PodName              string
	PodNamespace         string
	NodeName             string
	NodeRegion           string
	NodeZone             string
	TotalDataItems       int
	LocalDataItems       int
	SameRegionDataItems  int
	CrossRegionDataItems int
	TotalDataSize        int64
	LocalDataSize        int64
	SameRegionDataSize   int64
	CrossRegionDataSize  int64
	DataLocalityScore    int
	LastUpdated          time.Time
}

type RegionTransferStats struct {
	Region                string
	TotalDataTransferred  int64
	IntraRegionTransfer   int64
	IncomingTransfer      int64
	OutgoingTransfer      int64
	NodeCount             int
	EdgeNodeCount         int
	CloudNodeCount        int
	LocalDataPercentage   float64
	CrossRegionPercentage float64
}

func NewSchedulerMetricsCollector() *SchedulerMetricsCollector {
	return &SchedulerMetricsCollector{
		schedulingDecisions:      make([]SchedulingDecision, 0, 100),
		podDataLocalityStats:     make(map[string]PodDataLocalityStats),
		nodeDataLocalityScore:    make(map[string]float64),
		regionTransferStatistics: make(map[string]RegionTransferStats),
	}
}

func (c *SchedulerMetricsCollector) RecordSchedulingDecision(decision SchedulingDecision) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.schedulingDecisions) >= 100 {
		c.schedulingDecisions = c.schedulingDecisions[1:]
	}
	c.schedulingDecisions = append(c.schedulingDecisions, decision)

	c.podDataLocalityStats[decision.PodUID] = PodDataLocalityStats{
		PodName:              decision.PodName,
		PodNamespace:         decision.PodNamespace,
		NodeName:             decision.NodeName,
		NodeRegion:           decision.NodeRegion,
		NodeZone:             decision.NodeZone,
		TotalDataItems:       len(decision.DataItems),
		LocalDataItems:       decision.LocalDataItems,
		SameRegionDataItems:  decision.SameRegionDataItems,
		CrossRegionDataItems: decision.CrossRegionDataItems,
		TotalDataSize:        decision.TotalDataItemSize,
		LocalDataSize:        decision.LocalDataSize,
		SameRegionDataSize:   decision.SameRegionDataSize,
		CrossRegionDataSize:  decision.CrossRegionDataSize,
		DataLocalityScore:    decision.DataLocalityScore,
		LastUpdated:          decision.Timestamp,
	}

	currentScore := float64(decision.DataLocalityScore)
	if existingScore, ok := c.nodeDataLocalityScore[decision.NodeName]; ok {
		c.nodeDataLocalityScore[decision.NodeName] = existingScore*0.75 + currentScore*0.25
	} else {
		c.nodeDataLocalityScore[decision.NodeName] = currentScore
	}

	if stats, ok := c.regionTransferStatistics[decision.NodeRegion]; ok {
		stats.TotalDataTransferred += decision.TotalDataItemSize
		stats.IntraRegionTransfer += decision.LocalDataSize + decision.SameRegionDataSize
		stats.LocalDataPercentage = float64(stats.IntraRegionTransfer) / float64(stats.TotalDataTransferred)
		stats.CrossRegionPercentage = 1.0 - stats.LocalDataPercentage
		c.regionTransferStatistics[decision.NodeRegion] = stats
	} else {
		c.regionTransferStatistics[decision.NodeRegion] = RegionTransferStats{
			Region:                decision.NodeRegion,
			TotalDataTransferred:  decision.TotalDataItemSize,
			IntraRegionTransfer:   decision.LocalDataSize + decision.SameRegionDataSize,
			LocalDataPercentage:   float64(decision.LocalDataSize+decision.SameRegionDataSize) / float64(decision.TotalDataItemSize),
			CrossRegionPercentage: float64(decision.CrossRegionDataSize) / float64(decision.TotalDataItemSize),
		}
	}
}

func (c *SchedulerMetricsCollector) GetDataLocalityStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalPods := len(c.podDataLocalityStats)
	if totalPods == 0 {
		return map[string]interface{}{
			"schedulingDecisionsCount": 0,
			"noDataAvailable":          true,
		}
	}

	var totalLocalItems, totalSameRegionItems, totalCrossRegionItems int
	var totalDataSize, totalLocalSize, totalSameRegionSize, totalCrossRegionSize int64
	var avgDataLocalityScore float64

	for _, stats := range c.podDataLocalityStats {
		totalLocalItems += stats.LocalDataItems
		totalSameRegionItems += stats.SameRegionDataItems
		totalCrossRegionItems += stats.CrossRegionDataItems

		totalDataSize += stats.TotalDataSize
		totalLocalSize += stats.LocalDataSize
		totalSameRegionSize += stats.SameRegionDataSize
		totalCrossRegionSize += stats.CrossRegionDataSize

		avgDataLocalityScore += float64(stats.DataLocalityScore)
	}

	avgDataLocalityScore /= float64(totalPods)

	var localPercentage, sameRegionPercentage, crossRegionPercentage float64

	if totalDataSize > 0 {
		localPercentage = float64(totalLocalSize) / float64(totalDataSize) * 100
		sameRegionPercentage = float64(totalSameRegionSize) / float64(totalDataSize) * 100
		crossRegionPercentage = float64(totalCrossRegionSize) / float64(totalDataSize) * 100
	}

	podsWithAllLocalData := 0
	podsWithMajorityLocalData := 0
	podsWithNoLocalData := 0

	for _, stats := range c.podDataLocalityStats {
		if stats.TotalDataItems == 0 {
			continue
		}

		localRatio := float64(stats.LocalDataItems) / float64(stats.TotalDataItems)

		if localRatio == 1.0 {
			podsWithAllLocalData++
		} else if localRatio >= 0.5 {
			podsWithMajorityLocalData++
		} else if localRatio == 0 {
			podsWithNoLocalData++
		}
	}

	return map[string]interface{}{
		"schedulingDecisionsCount": len(c.schedulingDecisions),
		"overallStats": map[string]interface{}{
			"totalPods":                 totalPods,
			"avgDataLocalityScore":      avgDataLocalityScore,
			"totalDataSize":             totalDataSize,
			"localDataSize":             totalLocalSize,
			"sameRegionDataSize":        totalSameRegionSize,
			"crossRegionDataSize":       totalCrossRegionSize,
			"localDataPercentage":       localPercentage,
			"sameRegionDataPercentage":  sameRegionPercentage,
			"crossRegionDataPercentage": crossRegionPercentage,
		},
		"podLocalityProfile": map[string]interface{}{
			"podsWithAllLocalData":      podsWithAllLocalData,
			"podsWithMajorityLocalData": podsWithMajorityLocalData,
			"podsWithNoLocalData":       podsWithNoLocalData,
		},
		"regionStats":            c.regionTransferStatistics,
		"nodeDataLocalityScores": c.nodeDataLocalityScore,
	}
}

func (c *SchedulerMetricsCollector) GetRecentSchedulingDecisions() []SchedulingDecision {
	c.mu.RLock()
	defer c.mu.RUnlock()

	decisions := make([]SchedulingDecision, len(c.schedulingDecisions))
	copy(decisions, c.schedulingDecisions)

	return decisions
}

func (s *Scheduler) SetupMetricsEndpoints(mux *http.ServeMux) {
	metricsCollector := NewSchedulerMetricsCollector()
	s.metricsCollector = metricsCollector

	mux.HandleFunc("/data-locality-stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := metricsCollector.GetDataLocalityStats()
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			klog.Errorf("Failed to encode data locality stats: %v", err)
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/recent-scheduling-decisions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		decisions := metricsCollector.GetRecentSchedulingDecisions()
		if err := json.NewEncoder(w).Encode(decisions); err != nil {
			klog.Errorf("Failed to encode scheduling decisions: %v", err)
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
		}
	})
}

func (s *Scheduler) recordSchedulingMetrics(pod *v1.Pod, nodeName string, dataLocalityScore int, inputData []DataDependency, outputData []DataDependency) {
	if s.metricsCollector == nil {
		return
	}

	node, err := s.clientset.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s for metrics: %v", nodeName, err)
		return
	}

	nodeType := "unknown"
	if typeVal, ok := node.Labels["node-capability/node-type"]; ok {
		nodeType = typeVal
	}

	nodeRegion := "unknown"
	if regionVal, ok := node.Labels["topology.kubernetes.io/region"]; ok {
		nodeRegion = regionVal
	}

	nodeZone := "unknown"
	if zoneVal, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
		nodeZone = zoneVal
	}

	var dataItems []DataItemLocation
	var localDataItems, sameRegionDataItems, crossRegionDataItems int
	var totalDataSize, localDataSize, sameRegionDataSize, crossRegionDataSize int64

	for _, data := range inputData {
		storageNodes := s.storageIndex.GetStorageNodesForData(data.URN)
		if len(storageNodes) == 0 {
			parts := strings.SplitN(data.URN, "/", 2)
			if len(parts) > 0 {
				bucket := parts[0]
				storageNodes = s.storageIndex.GetBucketNodes(bucket)
			}
		}

		isLocal := containsString(storageNodes, nodeName)
		isSameRegion := false

		if !isLocal {
			// Check if data is in the same region
			for _, storageNode := range storageNodes {
				storageNodeObj, err := s.clientset.CoreV1().Nodes().Get(context.Background(), storageNode, metav1.GetOptions{})
				if err != nil {
					continue
				}

				if storageNodeRegion, ok := storageNodeObj.Labels["topology.kubernetes.io/region"]; ok && storageNodeRegion == nodeRegion {
					isSameRegion = true
					break
				}
			}
		}

		dataItem := DataItemLocation{
			URN:          data.URN,
			Size:         data.SizeBytes,
			IsLocal:      isLocal,
			IsSameRegion: isSameRegion,
			Locations:    storageNodes,
		}

		dataItems = append(dataItems, dataItem)
		totalDataSize += data.SizeBytes

		if isLocal {
			localDataItems++
			localDataSize += data.SizeBytes
		} else if isSameRegion {
			sameRegionDataItems++
			sameRegionDataSize += data.SizeBytes
		} else {
			crossRegionDataItems++
			crossRegionDataSize += data.SizeBytes
		}
	}

	for _, data := range outputData {
		parts := strings.SplitN(data.URN, "/", 2)
		if len(parts) == 0 {
			continue
		}

		bucket := parts[0]
		storageNodes := s.storageIndex.GetBucketNodes(bucket)

		isLocal := containsString(storageNodes, nodeName)
		isSameRegion := false

		if !isLocal {
			// Check if data is in the same region
			for _, storageNode := range storageNodes {
				storageNodeObj, err := s.clientset.CoreV1().Nodes().Get(context.Background(), storageNode, metav1.GetOptions{})
				if err != nil {
					continue
				}

				if storageNodeRegion, ok := storageNodeObj.Labels["topology.kubernetes.io/region"]; ok && storageNodeRegion == nodeRegion {
					isSameRegion = true
					break
				}
			}
		}

		dataItem := DataItemLocation{
			URN:          data.URN,
			Size:         data.SizeBytes,
			IsLocal:      isLocal,
			IsSameRegion: isSameRegion,
			Locations:    storageNodes,
		}

		dataItems = append(dataItems, dataItem)
		totalDataSize += data.SizeBytes

		if isLocal {
			localDataItems++
			localDataSize += data.SizeBytes
		} else if isSameRegion {
			sameRegionDataItems++
			sameRegionDataSize += data.SizeBytes
		} else {
			crossRegionDataItems++
			crossRegionDataSize += data.SizeBytes
		}
	}

	dataLocalityPercentage := 0.0
	localAccessPercentage := 0.0

	if len(dataItems) > 0 {
		dataLocalityPercentage = float64(localDataItems) / float64(len(dataItems)) * 100
	}

	if totalDataSize > 0 {
		localAccessPercentage = float64(localDataSize) / float64(totalDataSize) * 100
	}

	decision := SchedulingDecision{
		PodName:              pod.Name,
		PodNamespace:         pod.Namespace,
		PodUID:               string(pod.UID),
		NodeName:             nodeName,
		NodeType:             nodeType,
		NodeRegion:           nodeRegion,
		NodeZone:             nodeZone,
		Timestamp:            time.Now(),
		DataLocalityScore:    dataLocalityScore,
		DataItems:            dataItems,
		LocalDataItems:       localDataItems,
		SameRegionDataItems:  sameRegionDataItems,
		CrossRegionDataItems: crossRegionDataItems,
		TotalDataItemSize:    totalDataSize,
		LocalDataSize:        localDataSize,
		SameRegionDataSize:   sameRegionDataSize,
		CrossRegionDataSize:  crossRegionDataSize,
		DataTransferSummary: DataTransferSummary{
			DataLocalityPercentage: dataLocalityPercentage,
			LocalAccessPercentage:  localAccessPercentage,
		},
		DecisionFactors: make(map[string]float64),
	}

	s.metricsCollector.RecordSchedulingDecision(decision)
}
