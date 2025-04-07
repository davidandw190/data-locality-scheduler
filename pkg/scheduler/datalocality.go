package scheduler

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

type DataDependency struct {
	URN            string
	SizeBytes      int64
	ProcessingTime int // optional-seconds
	Priority       int
	DataType       string
	Weight         float64
}

type DataLocalityConfig struct {
	InputDataWeight    float64
	OutputDataWeight   float64
	DataTransferWeight float64

	MaxScore     int
	DefaultScore int

	LocalBandwidth       float64
	SameZoneBandwidth    float64
	SameRegionBandwidth  float64
	CrossRegionBandwidth float64
}

func NewDataLocalityConfig() *DataLocalityConfig {
	return &DataLocalityConfig{
		InputDataWeight:      0.7,
		OutputDataWeight:     0.3,
		DataTransferWeight:   0.8,
		MaxScore:             MaxScore,
		DefaultScore:         DefaultScore,
		LocalBandwidth:       1e9,   // 1 GB/s
		SameZoneBandwidth:    500e6, // 500 MB/s
		SameRegionBandwidth:  200e6, // 200 MB/s
		CrossRegionBandwidth: 50e6,  // 50 MB/s
	}
}

type DataLocalityPriority struct {
	storageIndex   *storage.StorageIndex
	bandwidthGraph *storage.BandwidthGraph
	config         *DataLocalityConfig
}

func NewDataLocalityPriority(
	storageIndex *storage.StorageIndex,
	bandwidthGraph *storage.BandwidthGraph,
	config *DataLocalityConfig) *DataLocalityPriority {

	if config == nil {
		config = NewDataLocalityConfig()
	}

	return &DataLocalityPriority{
		storageIndex:   storageIndex,
		bandwidthGraph: bandwidthGraph,
		config:         config,
	}
}

func (p *DataLocalityPriority) Score(pod *v1.Pod, nodeName string) (int, error) {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("DataLocalityPriority.Score for pod %s/%s on node %s took %v",
			pod.Namespace, pod.Name, nodeName, time.Since(startTime))
	}()

	inputData, outputData, err := p.extractDataDependencies(pod)
	if err != nil {
		klog.Warningf("Failed to extract data dependencies for pod %s/%s: %v",
			pod.Namespace, pod.Name, err)
		return p.config.DefaultScore, nil
	}

	if len(inputData) == 0 && len(outputData) == 0 {
		return p.config.DefaultScore, nil
	}

	klog.V(4).Infof("Pod %s/%s has %d input dependencies and %d output dependencies",
		pod.Namespace, pod.Name, len(inputData), len(outputData))

	for i, dep := range inputData {
		klog.V(5).Infof("Input dependency %d: %s (size: %d bytes)", i+1, dep.URN, dep.SizeBytes)
	}

	storageNodes := make(map[string][]string)
	for _, data := range inputData {
		nodes := p.storageIndex.GetStorageNodesForData(data.URN)
		if len(nodes) == 0 {
			parts := strings.SplitN(data.URN, "/", 2)
			if len(parts) > 0 {
				bucket := parts[0]
				nodes = p.storageIndex.GetBucketNodes(bucket)
			}
		}
		storageNodes[data.URN] = nodes

		klog.V(4).Infof("Data %s is available on nodes: %v", data.URN, nodes)
	}

	isStorageNode := false
	for _, nodes := range storageNodes {
		if containsString(nodes, nodeName) {
			isStorageNode = true
			break
		}
	}

	if isStorageNode {
		klog.V(4).Infof("Node %s is a storage node for this pod's data", nodeName)
	}

	inputScore := p.calculateInputDataScore(inputData, nodeName)
	outputScore := p.calculateOutputDataScore(outputData, nodeName)

	var dataScore int
	if len(inputData) > 0 && len(outputData) > 0 {
		dataScore = int((float64(inputScore) * p.config.InputDataWeight) +
			(float64(outputScore) * p.config.OutputDataWeight))
	} else if len(inputData) > 0 {
		dataScore = inputScore
	} else {
		dataScore = outputScore
	}

	if dataScore > p.config.MaxScore {
		dataScore = p.config.MaxScore
	} else if dataScore < 0 {
		dataScore = 0
	}

	klog.V(4).Infof("DataLocalityPriority: Pod %s/%s on node %s - Input score: %d, Output score: %d, Final score: %d",
		pod.Namespace, pod.Name, nodeName, inputScore, outputScore, dataScore)

	return dataScore, nil
}

func (p *DataLocalityPriority) extractDataDependencies(pod *v1.Pod) ([]DataDependency, []DataDependency, error) {
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

func (p *DataLocalityPriority) calculateInputDataScore(inputData []DataDependency, nodeName string) int {
	if len(inputData) == 0 {
		return p.config.DefaultScore
	}

	var totalWeight float64
	var weightedScore float64
	var bestStorageNodes = make(map[string]string) // Cache for best storage nodes

	for _, data := range inputData {
		// nodes holding the data
		storageNodes := p.storageIndex.GetStorageNodesForData(data.URN)

		// if no storage nodes found, try to get the bucket nodes
		if len(storageNodes) == 0 {
			parts := strings.SplitN(data.URN, "/", 2)
			if len(parts) > 0 {
				bucket := parts[0]
				storageNodes = p.storageIndex.GetBucketNodes(bucket)

				klog.V(5).Infof("For data %s, bucket %s is hosted on nodes: %v",
					data.URN, bucket, storageNodes)
			}
		}

		// if still no storage nodes, use default score
		if len(storageNodes) == 0 {
			klog.V(3).Infof("No storage nodes found for %s, using default score", data.URN)
			weightedScore += float64(p.config.DefaultScore) * data.Weight
			totalWeight += data.Weight
			continue
		}

		// we check if data is directly on the node (ideal case)
		if containsString(storageNodes, nodeName) {
			klog.V(4).Infof("Data %s is co-located on node %s - optimal score", data.URN, nodeName)
			weightedScore += float64(p.config.MaxScore) * data.Weight
			totalWeight += data.Weight
			continue
		}

		bestTransferTime := float64(1e12) // very large initial value
		var bestStorageNode string

		if cachedNode, exists := bestStorageNodes[data.URN]; exists {
			transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(
				cachedNode, nodeName, data.SizeBytes)
			bestTransferTime = transferTime
			bestStorageNode = cachedNode
		} else {
			for _, storageNode := range storageNodes {
				transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(
					storageNode, nodeName, data.SizeBytes)
				if transferTime < bestTransferTime {
					bestTransferTime = transferTime
					bestStorageNode = storageNode
				}
			}

			if bestStorageNode != "" {
				bestStorageNodes[data.URN] = bestStorageNode
			}
		}

		klog.V(5).Infof("For data %s (size: %d): best storage node is %s with transfer time %.2f s",
			data.URN, data.SizeBytes, bestStorageNode, bestTransferTime)

		score := calculateScoreFromTransferTime(bestTransferTime, p.config.MaxScore)

		sizeFactor := 1.0
		if data.SizeBytes > 100*1024*1024 { // 100MB
			sizeFactor = 1.5 // 50% more important for large files
		}

		adjustedWeight := data.Weight * sizeFactor
		weightedScore += float64(score) * adjustedWeight
		totalWeight += adjustedWeight

		klog.V(5).Infof("Data %s scored %d on node %s (weighted: %.2f)",
			data.URN, score, nodeName, float64(score)*adjustedWeight)
	}

	if totalWeight == 0 {
		return p.config.DefaultScore
	}

	finalScore := int(weightedScore / totalWeight)
	klog.V(4).Infof("Final input data score for node %s: %d (from weighted score %.2f / total weight %.2f)",
		nodeName, finalScore, weightedScore, totalWeight)

	return finalScore
}

// func (p *DataLocalityPriority) calculateInputDataScore(inputData []DataDependency, nodeName string) int {
// 	if len(inputData) == 0 {
// 		return p.config.DefaultScore
// 	}

// 	var totalWeight float64
// 	var weightedScore float64
// 	var bestStorageNodes = make(map[string]string) // Cache for best storage nodes

// 	for _, data := range inputData {
// 		// nodes holding the data
// 		storageNodes := p.storageIndex.GetStorageNodesForData(data.URN)

// 		// if no storage nodes found, try to get the bucket nodes
// 		if len(storageNodes) == 0 {
// 			parts := strings.SplitN(data.URN, "/", 2)
// 			if len(parts) > 0 {
// 				storageNodes = p.storageIndex.GetBucketNodes(parts[0])
// 			}
// 		}

// 		// if still no storage nodes, use default score
// 		if len(storageNodes) == 0 {
// 			klog.V(3).Infof("No storage nodes found for %s, using default score", data.URN)
// 			weightedScore += float64(p.config.DefaultScore) * data.Weight
// 			totalWeight += data.Weight
// 			continue
// 		}

// 		// we check if data is directly on the node (ideal case)
// 		if containsString(storageNodes, nodeName) {
// 			klog.V(4).Infof("Data %s is co-located on node %s - optimal score", data.URN, nodeName)
// 			weightedScore += float64(p.config.MaxScore) * data.Weight
// 			totalWeight += data.Weight
// 			continue
// 		}

// 		bestTransferTime := float64(1e12) // very large initial value
// 		var bestStorageNode string

// 		if cachedNode, exists := bestStorageNodes[data.URN]; exists {
// 			transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(
// 				cachedNode, nodeName, data.SizeBytes)
// 			bestTransferTime = transferTime
// 			bestStorageNode = cachedNode
// 		} else {
// 			for _, storageNode := range storageNodes {
// 				transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(
// 					storageNode, nodeName, data.SizeBytes)
// 				if transferTime < bestTransferTime {
// 					bestTransferTime = transferTime
// 					bestStorageNode = storageNode
// 				}
// 			}

// 			if bestStorageNode != "" {
// 				bestStorageNodes[data.URN] = bestStorageNode
// 			}
// 		}

// 		klog.V(5).Infof("For data %s (size: %d): best storage node is %s with transfer time %.2f s",
// 			data.URN, data.SizeBytes, bestStorageNode, bestTransferTime)

// 		score := calculateScoreFromTransferTime(bestTransferTime, p.config.MaxScore)

// 		sizeFactor := 1.0
// 		if data.SizeBytes > 100*1024*1024 { // 100MB
// 			sizeFactor = 1.5 // 50% more important for large files
// 		}

// 		adjustedWeight := data.Weight * sizeFactor
// 		weightedScore += float64(score) * adjustedWeight
// 		totalWeight += adjustedWeight
// 	}

// 	if totalWeight == 0 {
// 		return p.config.DefaultScore
// 	}

// 	finalScore := int(weightedScore / totalWeight)
// 	klog.V(4).Infof("Final input data score for node %s: %d", nodeName, finalScore)

// 	return finalScore
// }

func (p *DataLocalityPriority) calculateOutputDataScore(outputData []DataDependency, nodeName string) int {
	if len(outputData) == 0 {
		return p.config.DefaultScore
	}

	var totalWeight float64
	var weightedScore float64

	for _, data := range outputData {
		parts := strings.SplitN(data.URN, "/", 2)
		if len(parts) == 0 {
			continue
		}

		bucket := parts[0]
		storageNodes := p.storageIndex.GetBucketNodes(bucket)

		if len(storageNodes) == 0 {
			weightedScore += float64(p.config.DefaultScore) * data.Weight
			totalWeight += data.Weight
			continue
		}

		bestTransferTime := float64(1e12) // very large initial value
		var bestStorageNode string

		if containsString(storageNodes, nodeName) {
			bestTransferTime = 0.001
			bestStorageNode = nodeName
		} else {
			for _, storageNode := range storageNodes {
				transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(
					nodeName, storageNode, data.SizeBytes)
				if transferTime < bestTransferTime {
					bestTransferTime = transferTime
					bestStorageNode = storageNode
				}
			}
		}

		klog.V(5).Infof("For pod output data %s: best storage node is %s with transfer time %.2f ms",
			data.URN, bestStorageNode, bestTransferTime*1000)

		score := calculateScoreFromTransferTime(bestTransferTime, p.config.MaxScore)

		weightedScore += float64(score) * data.Weight
		totalWeight += data.Weight
	}

	if totalWeight == 0 {
		return p.config.DefaultScore
	}

	return int(weightedScore / totalWeight)
}

func calculateDataWeight(data DataDependency) float64 {
	sizeWeight := math.Log1p(float64(data.SizeBytes)/float64(1024*1024)) + 1.0

	if sizeWeight < 1.0 {
		sizeWeight = 1.0
	}

	return sizeWeight
}

func calculateScoreFromTransferTime(transferTime float64, maxScore int) int {
	if transferTime <= 0.01 {
		return maxScore
	}

	//TODO: maybe this can be set as a config parameter

	maxThreshold := 20.0 // 20 seconds is considered very slow
	if transferTime >= maxThreshold {
		return 0
	}

	// exponential decay for scoring - smoother curve than linear
	score := float64(maxScore) * math.Exp(-transferTime/5.0)
	return int(score)
}
