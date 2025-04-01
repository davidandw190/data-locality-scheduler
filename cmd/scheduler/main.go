package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/scheduler"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var (
	version   = "0.1.5"
	buildTime = "2025-04-01"
)

func main() {
	klog.InitFlags(nil)

	config := scheduler.NewDefaultConfig()

	var (
		configFile               string
		kubeconfig               string
		master                   string
		showVersion              bool
		localBandwidthMBps       float64
		sameZoneBandwidthMBps    float64
		sameRegionBandwidthMBps  float64
		crossRegionBandwidthMBps float64
		edgeCloudBandwidthMBps   float64
	)

	// Basic flags
	flag.StringVar(&configFile, "config", "", "Path to configuration file (YAML)")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&master, "master", "", "The address of the Kubernetes API server")
	flag.StringVar(&config.SchedulerName, "scheduler-name", config.SchedulerName, "Name of the scheduler")
	flag.BoolVar(&config.EnableMockData, "enable-mock-data", config.EnableMockData, "Enable mock MinIO data creation for testing")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")

	// Performance and caching flags
	flag.IntVar(&config.PercentageOfNodesToScore, "percentage-nodes-to-score", config.PercentageOfNodesToScore,
		"Percentage of nodes to score (1-100)")
	flag.BoolVar(&config.ResourceCacheEnabled, "enable-resource-cache", config.ResourceCacheEnabled,
		"Enable resource usage caching")
	flag.DurationVar(&config.ResourceCacheExpiration, "resource-cache-expiration", config.ResourceCacheExpiration,
		"Expiration time for resource cache entries")
	flag.DurationVar(&config.RefreshInterval, "refresh-interval", config.RefreshInterval,
		"Interval for refreshing storage information")
	flag.DurationVar(&config.SchedulerInterval, "scheduler-interval", config.SchedulerInterval,
		"Interval between scheduling attempts")

	// Scoring weights
	flag.Float64Var(&config.ResourceWeight, "resource-weight", config.ResourceWeight,
		"Weight for resource availability score")
	flag.Float64Var(&config.NodeAffinityWeight, "node-affinity-weight", config.NodeAffinityWeight,
		"Weight for node affinity score")
	flag.Float64Var(&config.NodeTypeWeight, "node-type-weight", config.NodeTypeWeight,
		"Weight for node type (edge/cloud) score")
	flag.Float64Var(&config.CapabilitiesWeight, "capabilities-weight", config.CapabilitiesWeight,
		"Weight for node capabilities score")
	flag.Float64Var(&config.DataLocalityWeight, "data-locality-weight", config.DataLocalityWeight,
		"Weight for data locality score")

	// Bandwidth settings in MB/s (convert to bytes/s when storing in config)
	flag.Float64Var(&localBandwidthMBps, "local-bandwidth-mbps", config.LocalBandwidth/1e6,
		"Bandwidth for local (same-node) transfers in MB/s")
	flag.Float64Var(&sameZoneBandwidthMBps, "same-zone-bandwidth-mbps", config.SameZoneBandwidth/1e6,
		"Bandwidth for same-zone transfers in MB/s")
	flag.Float64Var(&sameRegionBandwidthMBps, "same-region-bandwidth-mbps", config.SameRegionBandwidth/1e6,
		"Bandwidth for same-region transfers in MB/s")
	flag.Float64Var(&crossRegionBandwidthMBps, "cross-region-bandwidth-mbps", config.CrossRegionBandwidth/1e6,
		"Bandwidth for cross-region transfers in MB/s")
	flag.Float64Var(&edgeCloudBandwidthMBps, "edge-cloud-bandwidth-mbps", config.EdgeCloudBandwidth/1e6,
		"Bandwidth for edge-to-cloud transfers in MB/s")

	// HTTP server config
	flag.IntVar(&config.HealthServerPort, "health-port", config.HealthServerPort,
		"Port for health check and API server")

	// Default resource settings
	flag.Int64Var(&config.DefaultCPURequest, "default-cpu-request", config.DefaultCPURequest,
		"Default CPU request in millicores for containers without explicit requests")
	flag.Int64Var(&config.DefaultMemoryRequest, "default-memory-request", config.DefaultMemoryRequest,
		"Default memory request in bytes for containers without explicit requests")

	// Debug flags
	flag.BoolVar(&config.VerboseLogging, "verbose", config.VerboseLogging, "Enable verbose logging")
	flag.BoolVar(&config.DetailedMetrics, "detailed-metrics", config.DetailedMetrics, "Collect detailed metrics")
	flag.BoolVar(&config.EnableProfiling, "enable-profiling", config.EnableProfiling, "Enable performance profiling")
	flag.BoolVar(&config.EnableAPIEndpoint, "enable-api", config.EnableAPIEndpoint, "Enable API endpoint for management")

	flag.Parse()
	flag.Parse()

	wasFlagPassed := func(name string) bool {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}
	if showVersion {
		fmt.Printf("Data Locality Scheduler v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	if config.VerboseLogging {
		klog.V(1).Info("Verbose logging enabled")
		if err := flag.Set("v", "4"); err != nil {
			klog.Warning("Failed to set verbose logging level")
		}
	}

	if configFile != "" {
		if wasFlagPassed("local-bandwidth-mbps") {
			config.LocalBandwidth = localBandwidthMBps * 1e6
		}
		if wasFlagPassed("same-zone-bandwidth-mbps") {
			config.SameZoneBandwidth = sameZoneBandwidthMBps * 1e6
		}
		if wasFlagPassed("same-region-bandwidth-mbps") {
			config.SameRegionBandwidth = sameRegionBandwidthMBps * 1e6
		}
		if wasFlagPassed("cross-region-bandwidth-mbps") {
			config.CrossRegionBandwidth = crossRegionBandwidthMBps * 1e6
		}
		if wasFlagPassed("edge-cloud-bandwidth-mbps") {
			config.EdgeCloudBandwidth = edgeCloudBandwidthMBps * 1e6
		}
	}
	if wasFlagPassed("same-region-bandwidth-mbps") {
		config.SameRegionBandwidth = sameRegionBandwidthMBps * 1e6
	}
	if wasFlagPassed("cross-region-bandwidth-mbps") {
		config.CrossRegionBandwidth = crossRegionBandwidthMBps * 1e6
	}
	if wasFlagPassed("edge-cloud-bandwidth-mbps") {
		config.EdgeCloudBandwidth = edgeCloudBandwidthMBps * 1e6
	}

	if err := config.Validate(); err != nil {
		klog.Fatalf("Invalid configuration: %s", err.Error())
	}

	if klog.V(2).Enabled() {
		config.DumpEffectiveConfig()
	} else {
		klog.Infof("Scheduler %s starting with %s refresh interval, %s scheduler interval",
			config.SchedulerName, config.RefreshInterval, config.SchedulerInterval)
		klog.Infof("Data locality enabled with weights: Resource=%.1f, NodeAffinity=%.1f, NodeType=%.1f, Capabilities=%.1f, DataLocality=%.1f",
			config.ResourceWeight, config.NodeAffinityWeight, config.NodeTypeWeight,
			config.CapabilitiesWeight, config.DataLocalityWeight)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		klog.Infof("Received %s signal, initiating graceful shutdown", sig)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		cancel()

		<-shutdownCtx.Done()
	}()

	var k8sConfig *rest.Config
	var err error

	if kubeconfig == "" {
		klog.Info("Using in-cluster configuration")
		k8sConfig, err = rest.InClusterConfig()
	} else {
		klog.Infof("Using kubeconfig: %s", kubeconfig)
		k8sConfig, err = clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}

	if err != nil {
		klog.Fatalf("Error building kubernetes config: %s", err.Error())
	}

	k8sConfig.QPS = 100
	k8sConfig.Burst = 200

	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		klog.Fatalf("Error creating kubernetes client: %s", err.Error())
	}

	sched := scheduler.NewScheduler(clientset, config)

	klog.Infof("Starting %s...", config.SchedulerName)
	if err := sched.Run(ctx); err != nil {
		klog.Fatalf("Error running scheduler: %s", err.Error())
	}

	klog.Info("Scheduler shutdown complete")
}
