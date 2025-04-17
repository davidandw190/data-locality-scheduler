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
		[]string{"source_node", "destination_node", "data_type", "transfer_type", "source_type", "destination_type"},
	)

	// Bandwidth utilization metrics
	bandwidthUtilization = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_bandwidth_utilization_bytes_per_second",
			Help: "Bandwidth utilization between nodes based on scheduling decisions",
		},
		[]string{"source_node", "destination_node", "transfer_type"},
	)

	// Transfer time metrics
	dataTransferTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_data_transfer_time_seconds",
			Help: "Estimated time to transfer data between nodes based on bandwidth",
		},
		[]string{"source_node", "destination_node", "transfer_type"},
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

	// Transfer type counts
	transferTypeCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_transfer_type_count",
			Help: "Count of transfers by type",
		},
		[]string{"transfer_type"},
	)
)

type NodeInfo struct {
	Name      string
	Type      string
	Region    string
	Zone      string
	IpAddress string
}

const (
	TransferTypeLocal       = "local"
	TransferTypeSameZone    = "same_zone"
	TransferTypeSameRegion  = "same_region"
	TransferTypeCrossRegion = "cross_region"
	TransferTypeEdgeCloud   = "edge_cloud"
	TransferTypeCloudEdge   = "cloud_edge"
)

// Bandwidth configuration from scheduler config
const (
	LocalBandwidth       = 1000000000.0 // 1 GB/s - from scheduler config
	SameZoneBandwidth    = 600000000.0  // 600 MB/s
	SameRegionBandwidth  = 200000000.0  // 200 MB/s
	CrossRegionBandwidth = 50000000.0   // 50 MB/s
	EdgeCloudBandwidth   = 25000000.0   // 25 MB/s
)

func init() {
	prometheus.MustRegister(nodeDataLocalityScore)
	prometheus.MustRegister(podPlacementByNodeType)
	prometheus.MustRegister(dataTransferBytes)
	prometheus.MustRegister(bandwidthUtilization)
	prometheus.MustRegister(dataTransferTimeSeconds)
	prometheus.MustRegister(resourceEfficiency)
	prometheus.MustRegister(edgeUtilizationRatio)
	prometheus.MustRegister(transferTypeCount)
}

func main() {
	var kubeconfig string
	var listenAddr string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file. If empty, uses in-cluster config")
	flag.StringVar(&listenAddr, "listen-address", ":8080", "Address to listen on for HTTP requests")
	flag.Parse()

	var config *rest.Config
	var err error

	if kubeconfig == "" {
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Error getting in-cluster config: %v", err)
		}
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Error building kubeconfig: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	go collectMetrics(clientset)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Printf("Starting metrics server on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func collectMetrics(clientset *kubernetes.Clientset) {
	for {
		ctx := context.Background()
		nodeInfoMap, err := buildNodeInfoMap(ctx, clientset)
		if err != nil {
			log.Printf("Error building node info map: %v", err)
		} else {
			collectNodeMetrics(ctx, clientset, nodeInfoMap)
			collectPodMetrics(ctx, clientset, nodeInfoMap)
			calculateEdgeUtilization(ctx, clientset, nodeInfoMap)
			calculateResourceEfficiency(ctx, clientset, nodeInfoMap)
		}

		time.Sleep(30 * time.Second)
	}
}

func buildNodeInfoMap(ctx context.Context, clientset *kubernetes.Clientset) (map[string]NodeInfo, error) {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	nodeInfoMap := make(map[string]NodeInfo)
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

		ipAddress := ""
		for _, address := range node.Status.Addresses {
			if address.Type == "InternalIP" {
				ipAddress = address.Address
				break
			}
		}

		nodeInfoMap[node.Name] = NodeInfo{
			Name:      node.Name,
			Type:      nodeType,
			Region:    region,
			Zone:      zone,
			IpAddress: ipAddress,
		}
	}

	return nodeInfoMap, nil
}

func determineTransferType(sourceNode, destNode NodeInfo) string {
	// If source and destination are the same node
	if sourceNode.Name == destNode.Name {
		return TransferTypeLocal
	}

	// Edge to cloud or cloud to edge
	if sourceNode.Type == "edge" && destNode.Type == "cloud" {
		return TransferTypeEdgeCloud
	}
	if sourceNode.Type == "cloud" && destNode.Type == "edge" {
		return TransferTypeCloudEdge
	}

	// Same zone
	if sourceNode.Zone == destNode.Zone {
		return TransferTypeSameZone
	}

	// Same region
	if sourceNode.Region == destNode.Region {
		return TransferTypeSameRegion
	}

	// Cross region
	return TransferTypeCrossRegion
}

func getBandwidthForTransferType(transferType string) float64 {
	switch transferType {
	case TransferTypeLocal:
		return LocalBandwidth
	case TransferTypeSameZone:
		return SameZoneBandwidth
	case TransferTypeSameRegion:
		return SameRegionBandwidth
	case TransferTypeEdgeCloud, TransferTypeCloudEdge:
		return EdgeCloudBandwidth
	case TransferTypeCrossRegion:
		return CrossRegionBandwidth
	default:
		return CrossRegionBandwidth
	}
}

func collectNodeMetrics(ctx context.Context, clientset *kubernetes.Clientset, nodeInfoMap map[string]NodeInfo) {
	for nodeName, nodeInfo := range nodeInfoMap {
		localityScore := 50.0 // Default score

		node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			log.Printf("Error getting node %s: %v", nodeName, err)
			continue
		}

		if storageType, ok := node.Labels["node-capability/storage-type"]; ok {
			if storageType == "object" {
				localityScore += 20.0
			}
		}

		bucketCount := 0
		for label := range node.Labels {
			if strings.HasPrefix(label, "node-capability/storage-bucket-") {
				bucketCount++
			}
		}

		if bucketCount > 0 {
			localityScore += float64(bucketCount) * 2.0
			if localityScore > 100.0 {
				localityScore = 100.0
			}
		}

		nodeDataLocalityScore.With(prometheus.Labels{
			"node":      nodeName,
			"node_type": nodeInfo.Type,
			"region":    nodeInfo.Region,
			"zone":      nodeInfo.Zone,
		}).Set(localityScore)
	}
}

func collectPodMetrics(ctx context.Context, clientset *kubernetes.Clientset, nodeInfoMap map[string]NodeInfo) {
	pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing pods: %v", err)
		return
	}

	podPlacementByNodeType.Reset()

	localTransfers := 0
	sameZoneTransfers := 0
	sameRegionTransfers := 0
	crossRegionTransfers := 0
	edgeCloudTransfers := 0

	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}

		nodeInfo, exists := nodeInfoMap[pod.Spec.NodeName]
		if !exists {
			log.Printf("Node info not found for %s", pod.Spec.NodeName)
			continue
		}

		podPlacementByNodeType.With(prometheus.Labels{
			"node_type":     nodeInfo.Type,
			"region":        nodeInfo.Region,
			"zone":          nodeInfo.Zone,
			"pod_namespace": pod.Namespace,
		}).Inc()

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
							log.Printf("Error parsing size for %s: %v, using default", dataURN, err)
						}

						dataType := "generic"
						if len(parts) > 4 {
							dataType = strings.TrimSpace(parts[4])
						}

						if strings.HasPrefix(key, "data.scheduler.thesis/input-") {
							sourceNodes := findStorageNodesForData(ctx, clientset, dataURN)

							if len(sourceNodes) > 0 {
								for _, sourceNodeName := range sourceNodes {
									sourceNodeInfo, sourceExists := nodeInfoMap[sourceNodeName]
									if !sourceExists {
										log.Printf("Source node info not found for %s", sourceNodeName)
										continue
									}

									transferType := determineTransferType(sourceNodeInfo, nodeInfo)

									dataTransferBytes.With(prometheus.Labels{
										"source_node":      sourceNodeName,
										"destination_node": pod.Spec.NodeName,
										"data_type":        dataType,
										"transfer_type":    transferType,
										"source_type":      sourceNodeInfo.Type,
										"destination_type": nodeInfo.Type,
									}).Add(float64(size))

									bandwidth := getBandwidthForTransferType(transferType)
									transferTime := float64(size) / bandwidth

									bandwidthUtilization.With(prometheus.Labels{
										"source_node":      sourceNodeName,
										"destination_node": pod.Spec.NodeName,
										"transfer_type":    transferType,
									}).Set(bandwidth)

									dataTransferTimeSeconds.With(prometheus.Labels{
										"source_node":      sourceNodeName,
										"destination_node": pod.Spec.NodeName,
										"transfer_type":    transferType,
									}).Set(transferTime)

									switch transferType {
									case TransferTypeLocal:
										localTransfers++
									case TransferTypeSameZone:
										sameZoneTransfers++
									case TransferTypeSameRegion:
										sameRegionTransfers++
									case TransferTypeCrossRegion:
										crossRegionTransfers++
									case TransferTypeEdgeCloud, TransferTypeCloudEdge:
										edgeCloudTransfers++
									}
								}
							}
						} else if strings.HasPrefix(key, "data.scheduler.thesis/output-") {
							destNodes := findStorageNodesForData(ctx, clientset, dataURN)

							if len(destNodes) > 0 {
								for _, destNodeName := range destNodes {
									destNodeInfo, destExists := nodeInfoMap[destNodeName]
									if !destExists {
										log.Printf("Destination node info not found for %s", destNodeName)
										continue
									}

									transferType := determineTransferType(nodeInfo, destNodeInfo)

									dataTransferBytes.With(prometheus.Labels{
										"source_node":      pod.Spec.NodeName,
										"destination_node": destNodeName,
										"data_type":        dataType,
										"transfer_type":    transferType,
										"source_type":      nodeInfo.Type,
										"destination_type": destNodeInfo.Type,
									}).Add(float64(size))

									bandwidth := getBandwidthForTransferType(transferType)
									transferTime := float64(size) / bandwidth

									bandwidthUtilization.With(prometheus.Labels{
										"source_node":      pod.Spec.NodeName,
										"destination_node": destNodeName,
										"transfer_type":    transferType,
									}).Set(bandwidth)

									dataTransferTimeSeconds.With(prometheus.Labels{
										"source_node":      pod.Spec.NodeName,
										"destination_node": destNodeName,
										"transfer_type":    transferType,
									}).Set(transferTime)

									switch transferType {
									case TransferTypeLocal:
										localTransfers++
									case TransferTypeSameZone:
										sameZoneTransfers++
									case TransferTypeSameRegion:
										sameRegionTransfers++
									case TransferTypeCrossRegion:
										crossRegionTransfers++
									case TransferTypeEdgeCloud, TransferTypeCloudEdge:
										edgeCloudTransfers++
									}
								}
							}
						}
					}
				}
			}
		}
	}

	transferTypeCount.With(prometheus.Labels{"transfer_type": TransferTypeLocal}).Add(float64(localTransfers))
	transferTypeCount.With(prometheus.Labels{"transfer_type": TransferTypeSameZone}).Add(float64(sameZoneTransfers))
	transferTypeCount.With(prometheus.Labels{"transfer_type": TransferTypeSameRegion}).Add(float64(sameRegionTransfers))
	transferTypeCount.With(prometheus.Labels{"transfer_type": TransferTypeCrossRegion}).Add(float64(crossRegionTransfers))
	transferTypeCount.With(prometheus.Labels{"transfer_type": TransferTypeEdgeCloud}).Add(float64(edgeCloudTransfers))
}

func findStorageNodesForData(ctx context.Context, clientset *kubernetes.Clientset, dataURN string) []string {
	parts := strings.SplitN(dataURN, "/", 2)
	if len(parts) == 0 {
		return nil
	}

	bucket := parts[0]
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node-capability/storage-bucket-" + bucket + "=true",
	})

	if err != nil {
		log.Printf("Error finding storage nodes for %s: %v", bucket, err)
		return nil
	}

	var nodeNames []string
	for _, node := range nodes.Items {
		nodeNames = append(nodeNames, node.Name)
	}

	return nodeNames
}

func calculateEdgeUtilization(ctx context.Context, clientset *kubernetes.Clientset, nodeInfoMap map[string]NodeInfo) {
	pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Error listing pods: %v", err)
		return
	}

	regionZonePodCounts := make(map[string]map[string]map[string]int)

	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}

		nodeInfo, exists := nodeInfoMap[pod.Spec.NodeName]
		if !exists {
			continue
		}

		region := nodeInfo.Region
		zone := nodeInfo.Zone
		nodeType := nodeInfo.Type

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

		regionZonePodCounts[region][zone]["total"]++
		if nodeType == "edge" {
			regionZonePodCounts[region][zone]["edge"]++
		} else if nodeType == "cloud" {
			regionZonePodCounts[region][zone]["cloud"]++
		}
	}

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

func calculateResourceEfficiency(ctx context.Context, clientset *kubernetes.Clientset, nodeInfoMap map[string]NodeInfo) {
	for nodeName := range nodeInfoMap {
		fieldSelector := "spec.nodeName=" + nodeName
		pods, err := clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fieldSelector,
		})
		if err != nil {
			log.Printf("Error listing pods on node %s: %v", nodeName, err)
			continue
		}

		node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			log.Printf("Error getting node %s: %v", nodeName, err)
			continue
		}

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
					totalCPURequests += 100 // Default 100m CPU
				}

				if container.Resources.Requests.Memory() != nil {
					totalMemoryRequests += container.Resources.Requests.Memory().Value()
				} else {
					totalMemoryRequests += 200 * 1024 * 1024 // Default 200Mi memory
				}
			}
		}

		cpuCapacity := node.Status.Capacity.Cpu().MilliValue()
		memoryCapacity := node.Status.Capacity.Memory().Value()

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

		resourceEfficiency.With(prometheus.Labels{
			"node":          nodeName,
			"resource_type": "cpu",
		}).Set(cpuEfficiency)

		resourceEfficiency.With(prometheus.Labels{
			"node":          nodeName,
			"resource_type": "memory",
		}).Set(memoryEfficiency)
	}
}
