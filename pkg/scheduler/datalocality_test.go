package scheduler

import (
	"fmt"
	"testing"

	"github.com/davidandw190/data-locality-scheduler/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestDataLocalityPriority_CalculateInputDataScore(t *testing.T) {
	storageIndex := storage.NewStorageIndex()

	// set up test bucket-node mappings
	storageIndex.RegisterBucket("region1-bucket", []string{"node-region1"})
	storageIndex.RegisterBucket("region2-bucket", []string{"node-region2"})
	storageIndex.RegisterBucket("shared-bucket", []string{"node-region1", "node-region2"})

	// set up test storage nodes with different regions
	storageIndex.RegisterOrUpdateStorageNode(&storage.StorageNode{
		Name:     "node-region1",
		NodeType: storage.StorageTypeEdge,
		Region:   "region-1",
		Zone:     "zone-1-edge",
		Buckets:  []string{"region1-bucket", "shared-bucket"},
	})

	storageIndex.RegisterOrUpdateStorageNode(&storage.StorageNode{
		Name:     "node-region2",
		NodeType: storage.StorageTypeEdge,
		Region:   "region-2",
		Zone:     "zone-2-edge",
		Buckets:  []string{"region2-bucket", "shared-bucket"},
	})

	// add some test data items
	storageIndex.AddDataItem(&storage.DataItem{
		URN:         "region1-bucket/test.dat",
		Size:        100 * 1024 * 1024, // 100MB
		Locations:   []string{"node-region1"},
		ContentType: "application/octet-stream",
	})

	storageIndex.AddDataItem(&storage.DataItem{
		URN:         "region2-bucket/test.dat",
		Size:        100 * 1024 * 1024, // 100MB
		Locations:   []string{"node-region2"},
		ContentType: "application/octet-stream",
	})

	storageIndex.AddDataItem(&storage.DataItem{
		URN:         "shared-bucket/test.dat",
		Size:        100 * 1024 * 1024, // 100MB
		Locations:   []string{"node-region1", "node-region2"},
		ContentType: "application/octet-stream",
	})

	// Create bandwidth graph with realistic values
	bandwidthGraph := storage.NewBandwidthGraph(50 * 1024 * 1024) // 50MB/s default
	bandwidthGraph.SetNodeTopology("node-region1", "region-1", "zone-1-edge", storage.StorageTypeEdge)
	bandwidthGraph.SetNodeTopology("node-region2", "region-2", "zone-2-edge", storage.StorageTypeEdge)
	bandwidthGraph.SetNodeTopology("compute-node-region1", "region-1", "zone-1-cloud", storage.StorageTypeCloud)
	bandwidthGraph.SetNodeTopology("compute-node-region2", "region-2", "zone-2-cloud", storage.StorageTypeCloud)

	// Configure bandwidth between nodes
	bandwidthGraph.SetBandwidth("node-region1", "compute-node-region1", 200*1024*1024, 1.0) // 200MB/s, 1ms
	bandwidthGraph.SetBandwidth("node-region1", "compute-node-region2", 50*1024*1024, 30.0) // 50MB/s, 30ms
	bandwidthGraph.SetBandwidth("node-region2", "compute-node-region1", 50*1024*1024, 30.0) // 50MB/s, 30ms
	bandwidthGraph.SetBandwidth("node-region2", "compute-node-region2", 200*1024*1024, 1.0) // 200MB/s, 1ms

	// Create data locality priority calculator
	dataLocalityConfig := NewDataLocalityConfig()
	dataLocalityPriority := NewDataLocalityPriority(storageIndex, bandwidthGraph, dataLocalityConfig)

	// Test cases
	testCases := []struct {
		name             string
		inputData        []DataDependency
		nodeName         string
		expectedScore    int
		scoreExplanation string
	}{
		{
			name: "Same node data locality",
			inputData: []DataDependency{
				{
					URN:       "region1-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5,
					DataType:  "test",
					Weight:    1.0,
				},
			},
			nodeName:         "node-region1",
			expectedScore:    100, // Perfect score - data is on the same node
			scoreExplanation: "Data is co-located on the node, so perfect score (100)",
		},
		{
			name: "Same region data locality",
			inputData: []DataDependency{
				{
					URN:       "region1-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5,
					DataType:  "test",
					Weight:    1.0,
				},
			},
			nodeName:         "compute-node-region1",
			expectedScore:    65, // High score - data is in the same region
			scoreExplanation: "Data is in the same region, high bandwidth (200MB/s)",
		},
		{
			name: "Cross region data locality",
			inputData: []DataDependency{
				{
					URN:       "region1-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5,
					DataType:  "test",
					Weight:    1.0,
				},
			},
			nodeName:         "compute-node-region2",
			expectedScore:    40, // Lower score - data is in a different region
			scoreExplanation: "Data is in a different region, lower bandwidth (50MB/s)",
		},
		{
			name: "Multiple data sources",
			inputData: []DataDependency{
				{
					URN:       "region1-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5,
					DataType:  "test",
					Weight:    1.0,
				},
				{
					URN:       "region2-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5,
					DataType:  "test",
					Weight:    1.0,
				},
			},
			nodeName:         "compute-node-region1",
			expectedScore:    60, // Balanced score - one data source is close, one is far
			scoreExplanation: "Average of 75 (region1) and 40 (region2)",
		},
		{
			name: "High priority data",
			inputData: []DataDependency{
				{
					URN:       "region1-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  8, // High priority
					DataType:  "test",
					Weight:    1.0,
				},
				{
					URN:       "region2-bucket/test.dat",
					SizeBytes: 100 * 1024 * 1024,
					Priority:  5, // Normal priority
					DataType:  "test",
					Weight:    1.0,
				},
			},
			nodeName:         "compute-node-region1",
			expectedScore:    65, // Weighted toward region1 due to priority
			scoreExplanation: "Higher weight for region1 due to priority",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			score := dataLocalityPriority.calculateInputDataScore(tc.inputData, tc.nodeName)

			// Allow for some variance in scores due to implementation details
			assert.InDelta(t, tc.expectedScore, score, 15,
				"Expected score %d for %s, got %d. %s",
				tc.expectedScore, tc.nodeName, score, tc.scoreExplanation)
		})
	}
}

func TestScoreFromTransferTime(t *testing.T) {
	testCases := []struct {
		transferTime float64
		maxScore     int
		expected     int
	}{
		{0.001, 100, 100}, // Nearly instant transfer
		{0.5, 100, 90},    // Fast transfer
		{1.0, 100, 82},    // Good transfer
		{5.0, 100, 37},    // Moderate transfer
		{10.0, 100, 13},   // Slow transfer
		{20.0, 100, 2},    // Very slow transfer
		{30.0, 100, 0},    // Extremely slow
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Transfer time %.2fs", tc.transferTime), func(t *testing.T) {
			score := calculateScoreFromTransferTime(tc.transferTime, tc.maxScore)
			assert.InDelta(t, tc.expected, score, 5,
				"Transfer time %.2fs should score around %d, got %d",
				tc.transferTime, tc.expected, score)
		})
	}
}
