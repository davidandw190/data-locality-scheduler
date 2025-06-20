package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/davidandw190/data-locality-scheduler/pkg/daemon"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var (
	version   = "0.1.7"
	buildTime = "2025-04-25"
)

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var nodeName string
	var collectionInterval int
	var healthServerPort int
	var showVersion bool
	var enableDataLocality bool
	var edgeNode bool
	var region string
	var zone string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&nodeName, "node-name", "", "Name of the node this daemon is running on")
	flag.IntVar(&collectionInterval, "collection-interval", 60, "Interval between capability collections in seconds")
	flag.IntVar(&healthServerPort, "health-port", 8080, "Port to serve health and metrics endpoints")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&enableDataLocality, "enable-data-locality", true, "Enable data locality detection")
	flag.BoolVar(&edgeNode, "edge-node", false, "Mark this node as an edge node")
	flag.StringVar(&region, "region", "", "Region this node belongs to")
	flag.StringVar(&zone, "zone", "", "Zone this node belongs to")

	flag.Parse()

	if showVersion {
		fmt.Printf("Node Capability Daemon v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	if nodeName == "" {
		nodeName = os.Getenv("NODE_NAME")
		if nodeName == "" {
			klog.Fatal("Node name must be specified either via --node-name flag or NODE_NAME environment variable")
		}
	}

	if region == "" {
		region = os.Getenv("NODE_REGION")
	}

	if zone == "" {
		zone = os.Getenv("NODE_ZONE")
	}

	var config *rest.Config
	var err error

	if kubeconfig == "" {
		klog.Info("Using in-cluster configuration")
		config, err = rest.InClusterConfig()
	} else {
		klog.Infof("Using kubeconfig: %s", kubeconfig)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		klog.Infof("Received signal %v, shutting down", sig)
		cancel()
	}()

	var collectors []daemon.NodeCollector

	// primary node capability collector
	capCollector := daemon.NewNodeCapabilityCollector(nodeName, clientset)
	collectors = append(collectors, capCollector)

	// optional data locality collector
	var dataCollector daemon.NodeCollector
	if enableDataLocality {
		dataCollector = daemon.NewDataLocalityCollector(nodeName, clientset)
		collectors = append(collectors, dataCollector)
	}

	if edgeNode || region != "" || zone != "" {
		updateTopologyLabels(ctx, clientset, nodeName, edgeNode, region, zone)
	}

	healthServer := startHealthServer(healthServerPort, collectors)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		healthServer.Shutdown(shutdownCtx)
	}()

	klog.Info("Performing initial capability collection")
	for _, collector := range collectors {
		klog.Infof("Running initial collection for %s", collector.Name())
		labels, err := collector.Collect(ctx)
		if err != nil {
			klog.Errorf("Error during initial collection for %s: %v", collector.Name(), err)
			continue
		}
		if len(labels) > 0 {
			updateNodeLabels(ctx, clientset, nodeName, labels)
		}
	}

	klog.Infof("Node Capability Daemon v%s started on node %s", version, nodeName)
	klog.Infof("Collection interval: %d seconds", collectionInterval)
	klog.Infof("Data locality detection: %v", enableDataLocality)

	if edgeNode {
		klog.Info("This node is marked as an edge node")
	}
	if region != "" {
		klog.Infof("Region: %s", region)
	}
	if zone != "" {
		klog.Infof("Zone: %s", zone)
	}

	ticker := time.NewTicker(time.Duration(collectionInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, collector := range collectors {
				labels, err := collector.Collect(ctx)
				if err != nil {
					klog.Errorf("Error in collection for %s: %v", collector.Name(), err)
					continue
				}
				if len(labels) > 0 {
					updateNodeLabels(ctx, clientset, nodeName, labels)
				} else {
					klog.V(4).Infof("No label updates from %s", collector.Name())
				}
			}

		case <-ctx.Done():
			klog.Info("Node capability daemon shutting down")
			return
		}
	}
}

func updateTopologyLabels(ctx context.Context, clientset kubernetes.Interface, nodeName string, edgeNode bool, region, zone string) {
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get node %s: %v", nodeName, err)
		return
	}

	updatedNode := node.DeepCopy()
	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	updated := false

	// edge/cloud label
	if edgeNode {
		if updatedNode.Labels[daemon.EdgeNodeLabel] != daemon.EdgeNodeValue {
			updatedNode.Labels[daemon.EdgeNodeLabel] = daemon.EdgeNodeValue
			updated = true
		}
	} else {
		if updatedNode.Labels[daemon.EdgeNodeLabel] != daemon.CloudNodeValue {
			updatedNode.Labels[daemon.EdgeNodeLabel] = daemon.CloudNodeValue
			updated = true
		}
	}

	// region label
	if region != "" {
		if updatedNode.Labels[daemon.RegionLabel] != region {
			updatedNode.Labels[daemon.RegionLabel] = region
			updated = true
		}
	}

	// zone label
	if zone != "" {
		if updatedNode.Labels[daemon.ZoneLabel] != zone {
			updatedNode.Labels[daemon.ZoneLabel] = zone
			updated = true
		}
	}

	if updated {
		_, err = clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update node topology labels: %v", err)
		} else {
			klog.Info("Updated node topology labels")
		}
	}
}

// updateNodeLabels updates storage and bandwidth labels
func updateNodeLabels(ctx context.Context, clientset kubernetes.Interface, nodeName string, labels map[string]string) {
	if len(labels) == 0 {
		return
	}

	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get node %s: %v", nodeName, err)
		return
	}

	updatedNode := node.DeepCopy()
	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	updated := false
	for key, value := range labels {
		if value == "" {
			if _, exists := updatedNode.Labels[key]; exists {
				delete(updatedNode.Labels, key)
				updated = true
			}
		} else if updatedNode.Labels[key] != value {
			updatedNode.Labels[key] = value
			updated = true
		}
	}

	if !updated {
		klog.V(4).Info("No label changes detected, skipping update")
		return
	}

	_, err = clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update node labels: %v", err)
	} else {
		klog.Infof("Updated %d node labels", len(labels))
	}
}

func startHealthServer(port int, collectors []daemon.NodeCollector) *http.Server {
	mux := http.NewServeMux()

	// health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// readiness check endpoint
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	// capabilities endpoint
	mux.HandleFunc("/capabilities", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		w.Write([]byte("{\n"))

		for i, collector := range collectors {
			capabilities := collector.GetCapabilities()

			if i > 0 {
				w.Write([]byte(",\n"))
			}
			w.Write([]byte(fmt.Sprintf("  \"%s\": {\n", collector.Name())))

			j := 0
			for key, value := range capabilities {
				comma := ""
				if j < len(capabilities)-1 {
					comma = ","
				}

				value = strings.Replace(value, "\"", "\\\"", -1)
				w.Write([]byte(fmt.Sprintf("    \"%s\": \"%s\"%s\n", key, value, comma)))
				j++
			}

			w.Write([]byte("  }"))
		}

		w.Write([]byte("\n}\n"))
	})

	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(fmt.Appendf(nil, "{\n  \"version\": \"%s\",\n  \"buildTime\": \"%s\"\n}\n",
			version, buildTime))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		klog.Infof("Starting health server on :%d", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Health server failed: %v", err)
		}
	}()

	return server
}
