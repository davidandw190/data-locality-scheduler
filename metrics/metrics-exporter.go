package main

import (
	"context"
	"flag"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	// Node metrics
	nodeDataLocalityScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_node_data_locality_score",
			Help: "Data locality score for each node (0-100)",
		},
		[]string{"node", "node_type", "region", "zone"},
	)

	// Pod placement metrics
	podPlacementByNodeType = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_pod_placement_total",
			Help: "Count of pods placed on different node types",
		},
		[]string{"node_type", "region", "zone", "pod_namespace"},
	)

	// Data transfer metrics
	dataTransferBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_data_transfer_bytes_total",
			Help: "Amount of data transferred between nodes due to scheduling decisions",
		},
		[]string{"source_node", "destination_node", "data_type", "is_local", "is_cross_region"},
	)

	// Resource efficiency
	resourceEfficiency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_resource_efficiency",
			Help: "Resource efficiency score based on pod-node fit (0-100)",
		},
		[]string{"node", "resource_type"},
	)

	// Edge utilization ratio
	edgeUtilizationRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_edge_utilization_ratio",
			Help: "Ratio of pods scheduled on edge nodes (0.0-1.0)",
		},
		[]string{"region", "zone"},
	)
)

func init() {
	// Register the metrics with Prometheus
	prometheus.MustRegister(nodeDataLocalityScore)
	prometheus.MustRegister(podPlacementByNodeType)
	prometheus.MustRegister(dataTransferBytes)
	prometheus.MustRegister(resourceEfficiency)
	prometheus.MustRegister(edgeUtilizationRatio)
}

func main() {
	var kubeconfig string
	var listenAddr string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file. If empty, uses in-cluster config")
	flag.StringVar(&listenAddr, "listen-address", ":8080", "Address to listen on for HTTP requests")
	flag.Parse()

	// Create Kubernetes client
	var config *rest.Config
	var err error

	if kubeconfig == "" {
		// Use in-cluster config
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Error getting in-cluster config: %v", err)
		}
	} else {
		// Use kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Error building kubeconfig: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// Start metrics collection
	go collectMetrics(clientset)

	// Expose the metrics
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting metrics server on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func collectMetrics(clientset *kubernetes.Clientset) {
	for {
		ctx := context.Background()
		collectNodeMetrics(ctx, clientset)
		collectPodMetrics(ctx, clientset)
		calculateEdgeUtilization(ctx, clientset)
		calculateResourceEfficiency(ctx, clientset)

		// Sleep for 30 seconds before collecting metrics again
		time.Sleep(30 * time.Second)
	}
}

func collectNodeMetrics(ctx context.Context, clientset *kubernetes.Clientset) {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing nodes: %v", err)
		return
	}

	for _, node := range nodes.Items {
		nodeType := "unknown"
		if typeLabel, ok := node.Labels["node-capability/node-type"]; ok {
			nodeType = typeLabel
		}

		region := "unknown"
		if regionLabel, ok := node.Labels["topology.kubernetes.io/region"]; ok {
			region = regionLabel
		}

		zone := "unknown"
		if zoneLabel, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
			zone = zoneLabel
		}

		// Calculate data locality score based on node capabilities
		localityScore := 50.0 // Default score

		// Adjust score based on storage capability
		if storageType, ok := node.Labels["node-capability/storage-type"]; ok {
			if storageType == "object" {
				localityScore += 20.0
			}
		}

		// Count number of storage buckets on the node
		bucketCount := 0
		for label := range node.Labels {
			if strings.HasPrefix(label, "node-capability/storage-bucket-") {
				bucketCount++
			}
		}

		// Adjust score based on number of hosted buckets
		if bucketCount > 0 {
			localityScore += float64(bucketCount) * 2.0
			if localityScore > 100.0 {
				localityScore = 100.0
			}
		}

		// Record the score
		nodeDataLocalityScore.With(prometheus.Labels{
			"node":      node.Name,
			"node_type": nodeType,
			"region":    region,
			"zone":      zone,
		}).Set(localityScore)
	}
}

func collectPodMetrics(ctx context.Context, clientset *kubernetes.Clientset) {
	pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing pods: %v", err)
		return
	}

	// Reset counters for pod placement (to track changes)
	podPlacementByNodeType.Reset()

	nodeCache := make(map[string]map[string]string)

	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			// Pod hasn't been scheduled yet
			continue
		}

		// Get or cache node information
		nodeInfo, exists := nodeCache[pod.Spec.NodeName]
		if !exists {
			node, err := clientset.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
			if err != nil {
				log.Printf("Error getting node %s: %v", pod.Spec.NodeName, err)
				continue
			}

			nodeInfo = map[string]string{
				"node_type": "unknown",
				"region":    "unknown",
				"zone":      "unknown",
			}

			if typeLabel, ok := node.Labels["node-capability/node-type"]; ok {
				nodeInfo["node_type"] = typeLabel
			}
			if regionLabel, ok := node.Labels["topology.kubernetes.io/region"]; ok {
				nodeInfo["region"] = regionLabel
			}
			if zoneLabel, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
				nodeInfo["zone"] = zoneLabel
			}

			nodeCache[pod.Spec.NodeName] = nodeInfo
		}

		// Record pod placement
		podPlacementByNodeType.With(prometheus.Labels{
			"node_type":     nodeInfo["node_type"],
			"region":        nodeInfo["region"],
			"zone":          nodeInfo["zone"],
			"pod_namespace": pod.Namespace,
		}).Inc()

		// Check for data locality annotations to calculate data transfer metrics
		if pod.Annotations != nil {
			for key, value := range pod.Annotations {
				if strings.HasPrefix(key, "data.scheduler.thesis/input-") ||
					strings.HasPrefix(key, "data.scheduler.thesis/output-") {

					parts := strings.Split(value, ",")
					if len(parts) >= 2 {
						dataURN := strings.TrimSpace(parts[0])
						sizeStr := strings.TrimSpace(parts[1])

						size, err := strconv.ParseInt(sizeStr, 10, 64)
						if err != nil {
							size = 1024 * 1024 // 1MB default
						}

						// Determine data locality attributes
						sourceNode := "unknown"
						isLocal := "false"
						isCrossRegion := "false"
						dataType := "generic"

						if len(parts) > 4 {
							dataType = strings.TrimSpace(parts[4])
						}

						// Check if data is on same node (local)
						if strings.HasPrefix(key, "data.scheduler.thesis/input-") {
							// For input data, find where it's stored
							sourceBucket := strings.Split(dataURN, "/")[0]
							potentialNodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
								LabelSelector: "node-capability/storage-bucket-" + sourceBucket + "=true",
							})

							if err == nil && len(potentialNodes.Items) > 0 {
								sourceNode = potentialNodes.Items[0].Name

								if sourceNode == pod.Spec.NodeName {
									isLocal = "true"
								} else {
									// Check if cross-region
									sourceRegion := ""
									if regionLabel, ok := potentialNodes.Items[0].Labels["topology.kubernetes.io/region"]; ok {
										sourceRegion = regionLabel
									}

									if sourceRegion != "" && sourceRegion != nodeInfo["region"] {
										isCrossRegion = "true"
									}
								}
							}

							// Record the data transfer
							dataTransferBytes.With(prometheus.Labels{
								"source_node":      sourceNode,
								"destination_node": pod.Spec.NodeName,
								"data_type":        dataType,
								"is_local":         isLocal,
								"is_cross_region":  isCrossRegion,
							}).Add(float64(size))
						}
					}
				}
			}
		}
	}
}

func calculateEdgeUtilization(ctx context.Context, clientset *kubernetes.Clientset) {
	pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing pods: %v", err)
		return
	}

	// Track pods by region/zone
	regionZonePodCounts := make(map[string]map[string]map[string]int)

	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}

		node, err := clientset.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			continue
		}

		region := "unknown"
		if regionLabel, ok := node.Labels["topology.kubernetes.io/region"]; ok {
			region = regionLabel
		}

		zone := "unknown"
		if zoneLabel, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
			zone = zoneLabel
		}

		nodeType := "unknown"
		if typeLabel, ok := node.Labels["node-capability/node-type"]; ok {
			nodeType = typeLabel
		}

		// Initialize maps if needed
		if _, exists := regionZonePodCounts[region]; !exists {
			regionZonePodCounts[region] = make(map[string]map[string]int)
		}
		if _, exists := regionZonePodCounts[region][zone]; !exists {
			regionZonePodCounts[region][zone] = map[string]int{
				"edge":  0,
				"cloud": 0,
				"total": 0,
			}
		}

		// Increment pod count
		regionZonePodCounts[region][zone]["total"]++
		if nodeType == "edge" {
			regionZonePodCounts[region][zone]["edge"]++
		} else if nodeType == "cloud" {
			regionZonePodCounts[region][zone]["cloud"]++
		}
	}

	// Calculate edge utilization ratio by region/zone
	for region, zoneMap := range regionZonePodCounts {
		for zone, counts := range zoneMap {
			if counts["total"] > 0 {
				ratio := float64(counts["edge"]) / float64(counts["total"])
				edgeUtilizationRatio.With(prometheus.Labels{
					"region": region,
					"zone":   zone,
				}).Set(ratio)
			}
		}
	}
}

func calculateResourceEfficiency(ctx context.Context, clientset *kubernetes.Clientset) {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing nodes: %v", err)
		return
	}

	for _, node := range nodes.Items {
		// Get pods on this node
		fieldSelector := "spec.nodeName=" + node.Name
		pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fieldSelector,
		})
		if err != nil {
			log.Printf("Error listing pods on node %s: %v", node.Name, err)
			continue
		}

		// Calculate requested resources
		var totalCPURequests int64 = 0
		var totalMemoryRequests int64 = 0

		for _, pod := range pods.Items {
			if pod.Status.Phase != "Running" {
				continue
			}

			for _, container := range pod.Spec.Containers {
				if container.Resources.Requests.Cpu() != nil {
					totalCPURequests += container.Resources.Requests.Cpu().MilliValue()
				} else {
					totalCPURequests += 100 // Default 100m
				}

				if container.Resources.Requests.Memory() != nil {
					totalMemoryRequests += container.Resources.Requests.Memory().Value()
				} else {
					totalMemoryRequests += 200 * 1024 * 1024 // Default 200Mi
				}
			}
		}

		// Get node capacity
		cpuCapacity := node.Status.Capacity.Cpu().MilliValue()
		memoryCapacity := node.Status.Capacity.Memory().Value()

		// Calculate efficiency (higher is better)
		cpuEfficiency := 100.0
		memoryEfficiency := 100.0

		if cpuCapacity > 0 {
			cpuRatio := float64(totalCPURequests) / float64(cpuCapacity)
			if cpuRatio <= 0.9 {
				// Optimal range: 0-90% utilization gets high efficiency score
				cpuEfficiency = 100 - (math.Abs(0.7-cpuRatio) * 100)
			} else {
				// Overutilization gets lower efficiency score
				cpuEfficiency = 100 - ((cpuRatio - 0.9) * 1000)
			}
			if cpuEfficiency < 0 {
				cpuEfficiency = 0
			} else if cpuEfficiency > 100 {
				cpuEfficiency = 100
			}
		}

		if memoryCapacity > 0 {
			memRatio := float64(totalMemoryRequests) / float64(memoryCapacity)
			if memRatio <= 0.9 {
				// Optimal range: 0-90% utilization gets high efficiency score
				memoryEfficiency = 100 - (math.Abs(0.7-memRatio) * 100)
			} else {
				// Overutilization gets lower efficiency score
				memoryEfficiency = 100 - ((memRatio - 0.9) * 1000)
			}
			if memoryEfficiency < 0 {
				memoryEfficiency = 0
			} else if memoryEfficiency > 100 {
				memoryEfficiency = 100
			}
		}

		// Record efficiency metrics
		resourceEfficiency.With(prometheus.Labels{
			"node":          node.Name,
			"resource_type": "cpu",
		}).Set(cpuEfficiency)

		resourceEfficiency.With(prometheus.Labels{
			"node":          node.Name,
			"resource_type": "memory",
		}).Set(memoryEfficiency)
	}
}
