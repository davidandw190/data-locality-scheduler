package scheduler

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"
)

type SchedulerConfig struct {
	// Basic settings
	SchedulerName     string        `yaml:"schedulerName"`
	PodQueueSize      int           `yaml:"podQueueSize"`
	HealthServerPort  int           `yaml:"healthServerPort"`
	SchedulerInterval time.Duration `yaml:"schedulerInterval"`
	RefreshInterval   time.Duration `yaml:"refreshInterval"`
	EnableMockData    bool          `yaml:"enableMockData"`

	// Filtering options
	PercentageOfNodesToScore int `yaml:"percentageOfNodesToScore"`
	MinFeasibleNodesToFind   int `yaml:"minFeasibleNodesToFind"`
	MaxFailedAttempts        int `yaml:"maxFailedAttempts"`

	// Caching options
	ResourceCacheEnabled    bool          `yaml:"resourceCacheEnabled"`
	ResourceCacheExpiration time.Duration `yaml:"resourceCacheExpiration"`
	StorageCacheExpiration  time.Duration `yaml:"storageCacheExpiration"`

	// Weights
	ResourceWeight     float64 `yaml:"resourceWeight"`
	NodeAffinityWeight float64 `yaml:"nodeAffinityWeight"`
	NodeTypeWeight     float64 `yaml:"nodeTypeWeight"`
	CapabilitiesWeight float64 `yaml:"capabilitiesWeight"`
	DataLocalityWeight float64 `yaml:"dataLocalityWeight"`

	// Data-intensive weights
	DataIntensiveResourceWeight     float64 `yaml:"dataIntensiveResourceWeight"`
	DataIntensiveNodeAffinityWeight float64 `yaml:"dataIntensiveNodeAffinityWeight"`
	DataIntensiveNodeTypeWeight     float64 `yaml:"dataIntensiveNodeTypeWeight"`
	DataIntensiveCapabilitiesWeight float64 `yaml:"dataIntensiveCapabilitiesWeight"`
	DataIntensiveDataLocalityWeight float64 `yaml:"dataIntensiveDataLocalityWeight"`

	// Compute-intensive weights
	ComputeIntensiveResourceWeight     float64 `yaml:"computeIntensiveResourceWeight"`
	ComputeIntensiveNodeAffinityWeight float64 `yaml:"computeIntensiveNodeAffinityWeight"`
	ComputeIntensiveNodeTypeWeight     float64 `yaml:"computeIntensiveNodeTypeWeight"`
	ComputeIntensiveCapabilitiesWeight float64 `yaml:"computeIntensiveCapabilitiesWeight"`
	ComputeIntensiveDataLocalityWeight float64 `yaml:"computeIntensiveDataLocalityWeight"`

	// Bandwidth settings in bytes/second
	LocalBandwidth       float64 `yaml:"localBandwidth"`
	SameZoneBandwidth    float64 `yaml:"sameZoneBandwidth"`
	SameRegionBandwidth  float64 `yaml:"sameRegionBandwidth"`
	CrossRegionBandwidth float64 `yaml:"crossRegionBandwidth"`
	EdgeCloudBandwidth   float64 `yaml:"edgeCloudBandwidth"`

	// Latency settings in milliseconds
	LocalLatency       float64 `yaml:"localLatency"`
	SameZoneLatency    float64 `yaml:"sameZoneLatency"`
	SameRegionLatency  float64 `yaml:"sameRegionLatency"`
	CrossRegionLatency float64 `yaml:"crossRegionLatency"`
	EdgeCloudLatency   float64 `yaml:"edgeCloudLatency"`

	// Resource allocation options
	DefaultCPURequest    int64 `yaml:"defaultCPURequest"`    // in millicores
	DefaultMemoryRequest int64 `yaml:"defaultMemoryRequest"` // in bytes

	// Node scoring options
	MinResourceScore int `yaml:"minResourceScore"`
	MaxResourceScore int `yaml:"maxResourceScore"`
	DefaultScore     int `yaml:"defaultScore"`

	// Preemption options
	EnablePreemption bool `yaml:"enablePreemption"`

	// Debugging and logging options
	VerboseLogging    bool `yaml:"verboseLogging"`
	DetailedMetrics   bool `yaml:"detailedMetrics"`
	EnableProfiling   bool `yaml:"enableProfiling"`
	EnableAPIEndpoint bool `yaml:"enableAPIEndpoint"`
}

func NewDefaultConfig() *SchedulerConfig {
	return &SchedulerConfig{
		// Basic settings
		SchedulerName:     "data-locality-scheduler",
		PodQueueSize:      100,
		HealthServerPort:  8080,
		SchedulerInterval: 1 * time.Second,
		RefreshInterval:   5 * time.Minute,
		EnableMockData:    false,

		// Filtering options
		PercentageOfNodesToScore: 100,
		MinFeasibleNodesToFind:   100,
		MaxFailedAttempts:        5,

		// Caching options
		ResourceCacheEnabled:    true,
		ResourceCacheExpiration: 30 * time.Second,
		StorageCacheExpiration:  5 * time.Minute,

		// Weights for different scoring functions
		ResourceWeight:     DefaultResourceWeight,
		NodeAffinityWeight: DefaultNodeAffinityWeight,
		NodeTypeWeight:     DefaultNodeTypeWeight,
		CapabilitiesWeight: DefaultCapabilitiesWeight,
		DataLocalityWeight: DefaultDataLocalityWeight,

		// Data-intensive weights
		DataIntensiveResourceWeight:     DataIntensiveResourceWeight,
		DataIntensiveNodeAffinityWeight: DataIntensiveNodeAffinityWeight,
		DataIntensiveNodeTypeWeight:     DataIntensiveNodeTypeWeight,
		DataIntensiveCapabilitiesWeight: DataIntensiveCapabilitiesWeight,
		DataIntensiveDataLocalityWeight: DataIntensiveDataLocalityWeight,

		// Compute-intensive weights
		ComputeIntensiveResourceWeight:     ComputeIntensiveResourceWeight,
		ComputeIntensiveNodeAffinityWeight: ComputeIntensiveNodeAffinityWeight,
		ComputeIntensiveNodeTypeWeight:     ComputeIntensiveNodeTypeWeight,
		ComputeIntensiveCapabilitiesWeight: ComputeIntensiveCapabilitiesWeight,
		ComputeIntensiveDataLocalityWeight: ComputeIntensiveDataLocalityWeight,

		// Bandwidth settings in bytes/second
		LocalBandwidth:       1e9,   // 1 GB/s
		SameZoneBandwidth:    500e6, // 500 MB/s
		SameRegionBandwidth:  200e6, // 200 MB/s
		CrossRegionBandwidth: 50e6,  // 50 MB/s
		EdgeCloudBandwidth:   25e6,  // 25 MB/s

		// Latency settings in milliseconds
		LocalLatency:       0.1,  // 0.1ms
		SameZoneLatency:    1.0,  // 1ms
		SameRegionLatency:  5.0,  // 5ms
		CrossRegionLatency: 30.0, // 30ms
		EdgeCloudLatency:   40.0, // 40ms

		// Resource allocation options
		DefaultCPURequest:    100,               // 100m
		DefaultMemoryRequest: 200 * 1024 * 1024, // 200Mi

		// Node scoring options
		MinResourceScore: MinScore,
		MaxResourceScore: MaxScore,
		DefaultScore:     DefaultScore,

		// Preemption options
		EnablePreemption: false,

		// Debugging and logging options
		VerboseLogging:    false,
		DetailedMetrics:   false,
		EnableProfiling:   false,
		EnableAPIEndpoint: true,
	}
}

// LoadFromFile loads configuration from a YAML file
func (c *SchedulerConfig) LoadFromFile(filepath string) error {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	klog.V(4).Infof("Raw config content: %s", string(data))

	klog.V(4).Infof("Before unmarshaling: percentageOfNodesToScore=%d",
		c.PercentageOfNodesToScore)

	err = yaml.Unmarshal(data, c)
	if err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	klog.V(4).Infof("After unmarshaling: percentageOfNodesToScore=%d",
		c.PercentageOfNodesToScore)

	klog.Infof("Loaded configuration from file: %s", filepath)
	return nil
}

// LoadFromEnv overrides configuration with environment variables
func (c *SchedulerConfig) LoadFromEnv() {
	getEnv := func(key, defaultVal string) string {
		if val, exists := os.LookupEnv(key); exists {
			return val
		}
		return defaultVal
	}

	getEnvFloat := func(key string, defaultVal float64) float64 {
		strVal := getEnv(key, "")
		if strVal == "" {
			return defaultVal
		}
		if val, err := strconv.ParseFloat(strVal, 64); err == nil {
			return val
		}
		return defaultVal
	}

	getEnvInt := func(key string, defaultVal int) int {
		strVal := getEnv(key, "")
		if strVal == "" {
			return defaultVal
		}
		if val, err := strconv.Atoi(strVal); err == nil {
			return val
		}
		return defaultVal
	}

	getEnvBool := func(key string, defaultVal bool) bool {
		strVal := getEnv(key, "")
		if strVal == "" {
			return defaultVal
		}
		val := strings.ToLower(strVal)
		return val == "true" || val == "yes" || val == "1" || val == "on"
	}

	getEnvDuration := func(key string, defaultVal time.Duration) time.Duration {
		strVal := getEnv(key, "")
		if strVal == "" {
			return defaultVal
		}
		if val, err := time.ParseDuration(strVal); err == nil {
			return val
		}
		return defaultVal
	}

	// Basic settings
	c.SchedulerName = getEnv("SCHEDULER_NAME", c.SchedulerName)
	c.PodQueueSize = getEnvInt("POD_QUEUE_SIZE", c.PodQueueSize)
	c.HealthServerPort = getEnvInt("HEALTH_SERVER_PORT", c.HealthServerPort)
	c.SchedulerInterval = getEnvDuration("SCHEDULER_INTERVAL", c.SchedulerInterval)
	c.RefreshInterval = getEnvDuration("REFRESH_INTERVAL", c.RefreshInterval)
	c.EnableMockData = getEnvBool("ENABLE_MOCK_DATA", c.EnableMockData)

	// Filtering options
	c.PercentageOfNodesToScore = getEnvInt("PERCENTAGE_OF_NODES_TO_SCORE", c.PercentageOfNodesToScore)
	c.MinFeasibleNodesToFind = getEnvInt("MIN_FEASIBLE_NODES_TO_FIND", c.MinFeasibleNodesToFind)

	// Caching options
	c.ResourceCacheEnabled = getEnvBool("RESOURCE_CACHE_ENABLED", c.ResourceCacheEnabled)
	c.ResourceCacheExpiration = getEnvDuration("RESOURCE_CACHE_EXPIRATION", c.ResourceCacheExpiration)
	c.StorageCacheExpiration = getEnvDuration("STORAGE_CACHE_EXPIRATION", c.StorageCacheExpiration)

	// Weights
	c.ResourceWeight = getEnvFloat("RESOURCE_WEIGHT", c.ResourceWeight)
	c.NodeAffinityWeight = getEnvFloat("NODE_AFFINITY_WEIGHT", c.NodeAffinityWeight)
	c.NodeTypeWeight = getEnvFloat("NODE_TYPE_WEIGHT", c.NodeTypeWeight)
	c.CapabilitiesWeight = getEnvFloat("CAPABILITIES_WEIGHT", c.CapabilitiesWeight)
	c.DataLocalityWeight = getEnvFloat("DATA_LOCALITY_WEIGHT", c.DataLocalityWeight)

	// Bandwidth settings
	c.LocalBandwidth = getEnvFloat("LOCAL_BANDWIDTH", c.LocalBandwidth)
	c.SameZoneBandwidth = getEnvFloat("SAME_ZONE_BANDWIDTH", c.SameZoneBandwidth)
	c.SameRegionBandwidth = getEnvFloat("SAME_REGION_BANDWIDTH", c.SameRegionBandwidth)
	c.CrossRegionBandwidth = getEnvFloat("CROSS_REGION_BANDWIDTH", c.CrossRegionBandwidth)
	c.EdgeCloudBandwidth = getEnvFloat("EDGE_CLOUD_BANDWIDTH", c.EdgeCloudBandwidth)

	// Resource allocation options
	c.DefaultCPURequest = int64(getEnvInt("DEFAULT_CPU_REQUEST", int(c.DefaultCPURequest)))
	memStr := getEnv("DEFAULT_MEMORY_REQUEST", "")
	if memStr != "" {
		if mem, err := strconv.ParseInt(memStr, 10, 64); err == nil {
			c.DefaultMemoryRequest = mem
		} else {
			if strings.HasSuffix(memStr, "Mi") {
				if mem, err := strconv.ParseInt(strings.TrimSuffix(memStr, "Mi"), 10, 64); err == nil {
					c.DefaultMemoryRequest = mem * 1024 * 1024
				}
			} else if strings.HasSuffix(memStr, "Gi") {
				if mem, err := strconv.ParseInt(strings.TrimSuffix(memStr, "Gi"), 10, 64); err == nil {
					c.DefaultMemoryRequest = mem * 1024 * 1024 * 1024
				}
			}
		}
	}

	// Debugging options
	c.VerboseLogging = getEnvBool("VERBOSE_LOGGING", c.VerboseLogging)
	c.DetailedMetrics = getEnvBool("DETAILED_METRICS", c.DetailedMetrics)
	c.EnableProfiling = getEnvBool("ENABLE_PROFILING", c.EnableProfiling)

	klog.V(4).Info("Configuration updated from environment variables")
}

func (c *SchedulerConfig) Validate() error {
	if c.SchedulerName == "" {
		return fmt.Errorf("scheduler name cannot be empty")
	}

	if c.PodQueueSize <= 0 {
		return fmt.Errorf("pod queue size must be positive")
	}

	if c.HealthServerPort <= 0 || c.HealthServerPort > 65535 {
		return fmt.Errorf("health server port must be between 1 and 65535")
	}

	if c.SchedulerInterval <= 0 {
		return fmt.Errorf("scheduler interval must be positive")
	}

	if c.RefreshInterval <= 0 {
		return fmt.Errorf("refresh interval must be positive")
	}

	if c.PercentageOfNodesToScore <= 0 || c.PercentageOfNodesToScore > 100 {
		return fmt.Errorf("percentage of nodes to score must be between 1 and 100")
	}

	if c.ResourceCacheEnabled && c.ResourceCacheExpiration <= 0 {
		return fmt.Errorf("resource cache expiration must be positive")
	}

	if c.StorageCacheExpiration <= 0 {
		return fmt.Errorf("storage cache expiration must be positive")
	}

	weights := []float64{
		c.ResourceWeight,
		c.NodeAffinityWeight,
		c.NodeTypeWeight,
		c.CapabilitiesWeight,
		c.DataLocalityWeight,
	}

	for i, w := range weights {
		if w < 0 {
			return fmt.Errorf("weight at index %d must be non-negative", i)
		}
	}

	if c.LocalBandwidth <= 0 ||
		c.SameZoneBandwidth <= 0 ||
		c.SameRegionBandwidth <= 0 ||
		c.CrossRegionBandwidth <= 0 ||
		c.EdgeCloudBandwidth <= 0 {
		return fmt.Errorf("all bandwidth settings must be positive")
	}

	if c.LocalLatency < 0 ||
		c.SameZoneLatency < 0 ||
		c.SameRegionLatency < 0 ||
		c.CrossRegionLatency < 0 ||
		c.EdgeCloudLatency < 0 {
		return fmt.Errorf("all latency settings must be non-negative")
	}

	if c.DefaultCPURequest <= 0 {
		return fmt.Errorf("default CPU request must be positive")
	}

	if c.DefaultMemoryRequest <= 0 {
		return fmt.Errorf("default memory request must be positive")
	}

	if c.MinResourceScore < 0 || c.MaxResourceScore <= c.MinResourceScore {
		return fmt.Errorf("score range is invalid (min=%d, max=%d)", c.MinResourceScore, c.MaxResourceScore)
	}

	if c.DefaultScore < c.MinResourceScore || c.DefaultScore > c.MaxResourceScore {
		return fmt.Errorf("default score %d is outside valid range [%d, %d]",
			c.DefaultScore, c.MinResourceScore, c.MaxResourceScore)
	}

	return nil
}

func (c *SchedulerConfig) DumpEffectiveConfig() {
	configStr := c.String()

	lines := strings.Split(configStr, "\n")
	klog.Info("Effective configuration:")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			klog.Info(line)
		}
	}
}

func (c *SchedulerConfig) String() string {
	return fmt.Sprintf(`Configuration:
  Scheduler Name: %s
  Scheduler Interval: %v
  Refresh Interval: %v
  Enable Mock Data: %v
  
  Filtering:
    Percentage of Nodes to Score: %d%%
    Min Feasible Nodes to Find: %d
  
  Caching:
    Resource Cache Enabled: %v
    Resource Cache Expiration: %v
    Storage Cache Expiration: %v
  
  Weights:
    Resource: %.2f
    Node Affinity: %.2f
    Node Type: %.2f
    Capabilities: %.2f
    Data Locality: %.2f
  
  Bandwidth Settings:
    Local: %.2f MB/s
    Same Zone: %.2f MB/s
    Same Region: %.2f MB/s
    Cross Region: %.2f MB/s
    Edge-Cloud: %.2f MB/s
  
  Resource Defaults:
    CPU Request: %dm
    Memory Request: %dMi
  
  Scoring:
    Min Score: %d
    Max Score: %d
    Default Score: %d
  
  Debug:
    Verbose Logging: %v
    Detailed Metrics: %v
    Profiling Enabled: %v
`,
		c.SchedulerName,
		c.SchedulerInterval,
		c.RefreshInterval,
		c.EnableMockData,

		c.PercentageOfNodesToScore,
		c.MinFeasibleNodesToFind,

		c.ResourceCacheEnabled,
		c.ResourceCacheExpiration,
		c.StorageCacheExpiration,

		c.ResourceWeight,
		c.NodeAffinityWeight,
		c.NodeTypeWeight,
		c.CapabilitiesWeight,
		c.DataLocalityWeight,

		c.LocalBandwidth/1e6,
		c.SameZoneBandwidth/1e6,
		c.SameRegionBandwidth/1e6,
		c.CrossRegionBandwidth/1e6,
		c.EdgeCloudBandwidth/1e6,

		c.DefaultCPURequest,
		c.DefaultMemoryRequest/(1024*1024),

		c.MinResourceScore,
		c.MaxResourceScore,
		c.DefaultScore,

		c.VerboseLogging,
		c.DetailedMetrics,
		c.EnableProfiling)
}
