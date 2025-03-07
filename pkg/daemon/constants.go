// pkg/daemon/constants.go - New file
package daemon

import "time"

// Label prefixes and keys
const (
	// Base label prefix for all node capability labels
	LabelPrefix = "node-capability"

	// Storage labels
	StorageNodeLabel     = LabelPrefix + "/storage-service"
	StorageTypeLabel     = LabelPrefix + "/storage-type"
	StorageTechLabel     = LabelPrefix + "/storage-technology"
	StorageCapacityLabel = LabelPrefix + "/storage-capacity-bytes"
	BucketLabelPrefix    = LabelPrefix + "/storage-bucket-"

	// Network labels
	BandwidthPrefix = LabelPrefix + "/bandwidth-to-"
	LatencyPrefix   = LabelPrefix + "/latency-to-"

	// Node type labels
	EdgeNodeLabel  = LabelPrefix + "/node-type"
	EdgeNodeValue  = "edge"
	CloudNodeValue = "cloud"

	// Topology labels - from Kubernetes standard
	RegionLabel = "topology.kubernetes.io/region"
	ZoneLabel   = "topology.kubernetes.io/zone"

	// Capability scoring
	MaxScore     = 100
	MinScore     = 10
	DefaultScore = 50
)

// Time intervals
const (
	// Default collection intervals
	DefaultCapabilityCollectionInterval = 60 * time.Second
	DefaultBandwidthMeasurementInterval = 6 * time.Hour

	// Maximum number of nodes to measure bandwidth for
	MaxNodesToMeasure = 10
)
