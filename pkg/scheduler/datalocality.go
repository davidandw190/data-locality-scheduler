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
	ProcessingTime int // opt - seconds
	DataType       string
}

type DataLocalityConfig struct {
	InputDataWeight    float64
	DataTransferWeight float64
	MaxScore           int
	DefaultScore       int
}

type DataLocalityPriority struct {
	storageIndex   *storage.StorageIndex
	bandwidthGraph *storage.BandwidthGraph
	config         *DataLocalityConfig
}

func NewDataLocalityPriority(
	storageIndex *storage.StorageIndex,
	bandwidthGraph *storage.BandwidthGraph) *DataLocalityPriority {

	config := &DataLocalityConfig{
		InputDataWeight:    0.7, // 70% weight
		DataTransferWeight: 0.8, // 80% weight
		MaxScore:           100,
		DefaultScore:       50,
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

	inputScore := p.calculateInputDataScore(inputData, nodeName)

	outputScore := p.calculateOutputDataScore(outputData, nodeName)

	var dataScore int
	if len(inputData) > 0 && len(outputData) > 0 {
		dataScore = int((float64(inputScore) * p.config.InputDataWeight) +
			(float64(outputScore) * (1.0 - p.config.InputDataWeight)))
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

	for k, v := range pod.Annotations {
		if strings.HasPrefix(k, "data.scheduler.thesis/input-") {
			// format: urn,size_bytes[,processing_time[,priority[,data_type]]]
			parts := strings.Split(v, ",")
			if len(parts) < 2 {
				parseErrors = append(parseErrors, fmt.Sprintf("invalid format for %s: %s (need at least URN,size)", k, v))
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

			// optional processing time
			processingTime := 0
			if len(parts) > 2 {
				if pt, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
					processingTime = pt
				}
			}

			// optional data type
			dataType := "generic"
			if len(parts) > 4 {
				dataType = strings.TrimSpace(parts[4])
			}

			inputData = append(inputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: processingTime,
				DataType:       dataType,
			})
		} else if strings.HasPrefix(k, "data.scheduler.thesis/output-") {
			// format: urn,size_bytes[,processing_time[,priority[,data_type]]]
			parts := strings.Split(v, ",")
			if len(parts) < 2 {
				parseErrors = append(parseErrors, fmt.Sprintf("invalid format for %s: %s (need at least URN,size)", k, v))
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

			// optional processing time
			processingTime := 0
			if len(parts) > 2 {
				if pt, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
					processingTime = pt
				}
			}

			// optional data type
			dataType := "generic"
			if len(parts) > 4 {
				dataType = strings.TrimSpace(parts[4])
			}

			outputData = append(outputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: processingTime,
				DataType:       dataType,
			})
		}
	}

	if eoInput, ok := pod.Annotations["data.scheduler.thesis/eo-input"]; ok {
		parts := strings.Split(eoInput, ",")
		if len(parts) >= 2 {
			urn := strings.TrimSpace(parts[0])
			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				size = 100 * 1024 * 1024
			}

			inputData = append(inputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: 30,
				DataType:       "eo-imagery",
			})
		}
	}

	if eoOutput, ok := pod.Annotations["data.scheduler.thesis/eo-output"]; ok {
		parts := strings.Split(eoOutput, ",")
		if len(parts) >= 2 {
			urn := strings.TrimSpace(parts[0])
			size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				size = 50 * 1024 * 1024
			}

			outputData = append(outputData, DataDependency{
				URN:            urn,
				SizeBytes:      size,
				ProcessingTime: 0,
				DataType:       "cog",
			})
		}
	}

	if len(parseErrors) > 0 {
		return inputData, outputData, fmt.Errorf("data dependency parsing errors: %s", strings.Join(parseErrors, "; "))
	}

	return inputData, outputData, nil
}

func (p *DataLocalityPriority) calculateInputDataScore(inputData []DataDependency, nodeName string) int {
	if len(inputData) == 0 {
		return p.config.DefaultScore
	}

	var totalWeight float64
	var weightedScore float64
	var bestStorageNodes = make(map[string]string)

	for _, data := range inputData {
		weight := calculateDataWeight(data)
		storageNodes := p.storageIndex.GetStorageNodesForData(data.URN)

		if len(storageNodes) == 0 {
			parts := strings.SplitN(data.URN, "/", 2)
			if len(parts) > 0 {
				storageNodes = p.storageIndex.GetBucketNodes(parts[0])
			}
		}

		if len(storageNodes) == 0 {
			klog.V(3).Infof("No storage nodes found for %s, using default score", data.URN)
			weightedScore += float64(p.config.DefaultScore) * weight
			totalWeight += weight
			continue
		}

		if containsString(storageNodes, nodeName) {
			klog.V(4).Infof("Data %s is co-located on node %s - optimal score", data.URN, nodeName)
			weightedScore += float64(p.config.MaxScore) * weight
			totalWeight += weight
			continue
		}

		bestTransferTime := float64(1e12)
		var bestStorageNode string

		if cachedNode, exists := bestStorageNodes[data.URN]; exists {
			transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(cachedNode, nodeName, data.SizeBytes)
			bestTransferTime = transferTime
			bestStorageNode = cachedNode
		} else {
			for _, storageNode := range storageNodes {
				transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(storageNode, nodeName, data.SizeBytes)
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

		// opt
		sizeFactor := 1.0
		if data.SizeBytes > 100*1024*1024 { // 100MB
			sizeFactor = 1.5 // 50% more important for large files
		}

		weight *= sizeFactor
		//

		weightedScore += float64(score) * weight
		totalWeight += weight
	}

	if totalWeight == 0 {
		return p.config.DefaultScore
	}

	finalScore := int(weightedScore / totalWeight)
	klog.V(4).Infof("Final input data score for node %s: %d", nodeName, finalScore)

	return finalScore
}

func (p *DataLocalityPriority) calculateOutputDataScore(outputData []DataDependency, nodeName string) int {
	if len(outputData) == 0 {
		return p.config.DefaultScore
	}

	var totalWeight float64
	var weightedScore float64

	for _, data := range outputData {
		weight := calculateDataWeight(data)

		parts := strings.SplitN(data.URN, "/", 2)
		if len(parts) == 0 {
			continue
		}

		bucket := parts[0]
		storageNodes := p.storageIndex.GetBucketNodes(bucket)

		if len(storageNodes) == 0 {
			weightedScore += float64(p.config.DefaultScore) * weight
			totalWeight += weight
			continue
		}

		bestTransferTime := float64(1e12)
		var bestStorageNode string

		for _, storageNode := range storageNodes {
			if storageNode == nodeName {
				bestTransferTime = 0.001
				bestStorageNode = storageNode
				break
			}

			transferTime := p.bandwidthGraph.EstimateTransferTimeBetweenNodes(nodeName, storageNode, data.SizeBytes)
			if transferTime < bestTransferTime {
				bestTransferTime = transferTime
				bestStorageNode = storageNode
			}
		}

		klog.V(5).Infof("For pod output data %s: best storage node is %s with transfer time %.2f ms",
			data.URN, bestStorageNode, bestTransferTime*1000)

		score := calculateScoreFromTransferTime(bestTransferTime, p.config.MaxScore)

		weightedScore += float64(score) * weight
		totalWeight += weight
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

	maxThreshold := 20.0
	if transferTime >= maxThreshold {
		return 0
	}

	score := float64(maxScore) * math.Exp(-transferTime/5.0)
	return int(score)
}
