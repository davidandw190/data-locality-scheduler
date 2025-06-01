package scheduler

import "time"

// Priority weights
const (
	// Default weights
	DefaultResourceWeight     = 0.15
	DefaultNodeAffinityWeight = 0.10
	DefaultNodeTypeWeight     = 0.15
	DefaultCapabilitiesWeight = 0.10
	DefaultDataLocalityWeight = 0.50

	// Data-intensive weights
	DataIntensiveResourceWeight     = 0.10
	DataIntensiveNodeAffinityWeight = 0.05
	DataIntensiveNodeTypeWeight     = 0.05
	DataIntensiveCapabilitiesWeight = 0.05
	DataIntensiveDataLocalityWeight = 0.75

	// Compute-intensive weights
	ComputeIntensiveResourceWeight     = 0.40
	ComputeIntensiveNodeAffinityWeight = 0.15
	ComputeIntensiveNodeTypeWeight     = 0.10
	ComputeIntensiveCapabilitiesWeight = 0.15
	ComputeIntensiveDataLocalityWeight = 0.20
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
