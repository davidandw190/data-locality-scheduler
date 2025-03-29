package scheduler

import "time"

// Priority weights
const (
	// Default weights
	DefaultResourceWeight     = 0.3
	DefaultNodeAffinityWeight = 0.2
	DefaultNodeTypeWeight     = 0.1
	DefaultCapabilitiesWeight = 0.1
	DefaultDataLocalityWeight = 0.3

	// Data-intensive weights
	DataIntensiveResourceWeight     = 0.2
	DataIntensiveNodeAffinityWeight = 0.1
	DataIntensiveNodeTypeWeight     = 0.1
	DataIntensiveCapabilitiesWeight = 0.1
	DataIntensiveDataLocalityWeight = 0.5

	// Compute-intensive weights
	ComputeIntensiveResourceWeight     = 0.5
	ComputeIntensiveNodeAffinityWeight = 0.2
	ComputeIntensiveNodeTypeWeight     = 0.1
	ComputeIntensiveCapabilitiesWeight = 0.1
	ComputeIntensiveDataLocalityWeight = 0.1
)

const (
	MinScore     = 0
	MaxScore     = 100
	DefaultScore = 50
)

const (
	SchedulerInterval       = 1 * time.Second
	StorageRefreshInterval  = 5 * time.Minute
	BandwidthUpdateInterval = 1 * time.Hour
)

const (
	// Annotations
	AnnotationDataIntensive    = "scheduler.thesis/data-intensive"
	AnnotationComputeIntensive = "scheduler.thesis/compute-intensive"
	AnnotationPreferEdge       = "scheduler.thesis/prefer-edge"
	AnnotationPreferCloud      = "scheduler.thesis/prefer-cloud"
	AnnotationRequireGPU       = "scheduler.thesis/requires-gpu"
	AnnotationFastStorage      = "scheduler.thesis/requires-fast-storage"
	AnnotationPreferRegion     = "scheduler.thesis/prefer-region"
	AnnotationCapabilityPrefix = "scheduler.thesis/capability-"
)
