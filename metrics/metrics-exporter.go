package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	schedulingDecisions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "data_locality_scheduler_decisions_total",
		Help: "Total number of scheduling decisions made",
	})

	dataLocalityScore = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "data_locality_score",
		Help: "Data locality score for each pod (0-100)",
	}, []string{"pod_name", "pod_namespace", "node_name", "node_type", "region", "zone"})

	nodeDataLocalityScore = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_data_locality_score",
		Help: "Data locality score for each node (0-100)",
	}, []string{"node", "node_type", "region", "zone"})

	podPlacementByNodeType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_placement_total",
		Help: "Count of pods placed on different node types",
	}, []string{"node_type", "region", "zone", "pod_namespace"})

	dataItemLocalityCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "data_item_locality_count",
		Help: "Number of data items in each locality category",
	}, []string{"locality_type"}) // local, same_region, cross_region

	podLocalityProfile = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pod_locality_profile_count",
		Help: "Number of pods with different data locality profiles",
	}, []string{"profile"}) // all_local, majority_local, no_local

	dataStorageByLocality = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "data_storage_bytes_by_locality",
		Help: "Size of data in each locality category in bytes",
	}, []string{"locality_type"}) // local, same_region, cross_region

	dataLocalityPercentage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "data_locality_percentage",
		Help: "Percentage of data items that are local to their pods",
	})

	localDataAccessPercentage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "local_data_access_percentage",
		Help: "Percentage of data size accessible locally",
	})

	sameRegionDataPercentage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "same_region_data_percentage",
		Help: "Percentage of data size accessible within the same region",
	})

	crossRegionDataPercentage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cross_region_data_percentage",
		Help: "Percentage of data size accessible across regions",
	})

	regionDataLocalityPercentage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "region_data_locality_percentage",
		Help: "Percentage of data locality per region",
	}, []string{"region"})

	regionEdgeUtilization = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "region_edge_utilization_ratio",
		Help: "Ratio of pods scheduled on edge nodes per region",
	}, []string{"region"})

	resourceEfficiency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resource_efficiency",
		Help: "Resource efficiency score based on pod-node fit (0-100)",
	}, []string{"node", "resource_type"})
)

type Config struct {
	SchedulerURL    string
	MetricsInterval time.Duration
	ListenAddress   string
	KubeConfig      string
}

type NodeInfo struct {
	Name     string
	NodeType string
	Region   string
	Zone     string
}

func main() {
	var config Config

	flag.StringVar(&config.SchedulerURL, "scheduler-url", "http://data-locality-scheduler:8080", "URL of the data locality scheduler")
	flag.DurationVar(&config.MetricsInterval, "metrics-interval", 30*time.Second, "Interval for collecting metrics")
	flag.StringVar(&config.ListenAddress, "listen-address", ":8080", "Address to listen on for HTTP requests")
	flag.StringVar(&config.KubeConfig, "kubeconfig", "", "Path to kubeconfig file. If empty, uses in-cluster config")
	flag.Parse()

	log.Printf("Starting data locality metrics exporter with scheduler URL: %s", config.SchedulerURL)
	log.Printf("Metrics collection interval: %s", config.MetricsInterval)

	k8sClient, err := createK8sClient(config.KubeConfig)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	go collectMetrics(config, k8sClient)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Printf("Starting metrics server on %s", config.ListenAddress)
	log.Fatal(http.ListenAndServe(config.ListenAddress, nil))
}

func createK8sClient(kubeconfig string) (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error

	if kubeconfig == "" {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to create in-cluster config: %v", err)
		}
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}

	return clientset, nil
}

func collectMetrics(config Config, k8sClient *kubernetes.Clientset) {
	ticker := time.NewTicker(config.MetricsInterval)
	defer ticker.Stop()

	for {
		collectSchedulerMetrics(config.SchedulerURL)
		collectResourceUtilizationMetrics(k8sClient)
		collectEdgeUtilizationMetrics(k8sClient)

		<-ticker.C
	}
}

func collectSchedulerMetrics(schedulerURL string) {
	dataLocalityStatsURL := fmt.Sprintf("%s/data-locality-stats", schedulerURL)
	resp, err := http.Get(dataLocalityStatsURL)
	if err != nil {
		log.Printf("Error fetching data locality stats: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error response from scheduler: %s", resp.Status)
		return
	}

	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		log.Printf("Error decoding data locality stats: %v", err)
		return
	}

	if noData, ok := stats["noDataAvailable"].(bool); ok && noData {
		log.Printf("No data available from scheduler")
		return
	}

	if overallStats, ok := stats["overallStats"].(map[string]interface{}); ok {
		if localPercentage, ok := overallStats["localDataPercentage"].(float64); ok {
			dataLocalityPercentage.Set(localPercentage)
			localDataAccessPercentage.Set(localPercentage)
		}

		if sameRegionPercentage, ok := overallStats["sameRegionDataPercentage"].(float64); ok {
			sameRegionDataPercentage.Set(sameRegionPercentage)
		}

		if crossRegionPercentage, ok := overallStats["crossRegionDataPercentage"].(float64); ok {
			crossRegionDataPercentage.Set(crossRegionPercentage)
		}

		if totalLocalItems, ok := getFloat64(overallStats, "localDataItems"); ok {
			dataItemLocalityCount.WithLabelValues("local").Set(totalLocalItems)
		}

		if totalSameRegionItems, ok := getFloat64(overallStats, "sameRegionDataItems"); ok {
			dataItemLocalityCount.WithLabelValues("same_region").Set(totalSameRegionItems)
		}

		if totalCrossRegionItems, ok := getFloat64(overallStats, "crossRegionDataItems"); ok {
			dataItemLocalityCount.WithLabelValues("cross_region").Set(totalCrossRegionItems)
		}

		if localDataSize, ok := getFloat64(overallStats, "localDataSize"); ok {
			dataStorageByLocality.WithLabelValues("local").Set(localDataSize)
		}

		if sameRegionDataSize, ok := getFloat64(overallStats, "sameRegionDataSize"); ok {
			dataStorageByLocality.WithLabelValues("same_region").Set(sameRegionDataSize)
		}

		if crossRegionDataSize, ok := getFloat64(overallStats, "crossRegionDataSize"); ok {
			dataStorageByLocality.WithLabelValues("cross_region").Set(crossRegionDataSize)
		}
	}

	if podProfile, ok := stats["podLocalityProfile"].(map[string]interface{}); ok {
		if podsWithAllLocalData, ok := getFloat64(podProfile, "podsWithAllLocalData"); ok {
			podLocalityProfile.WithLabelValues("all_local").Set(podsWithAllLocalData)
		}

		if podsWithMajorityLocalData, ok := getFloat64(podProfile, "podsWithMajorityLocalData"); ok {
			podLocalityProfile.WithLabelValues("majority_local").Set(podsWithMajorityLocalData)
		}

		if podsWithNoLocalData, ok := getFloat64(podProfile, "podsWithNoLocalData"); ok {
			podLocalityProfile.WithLabelValues("no_local").Set(podsWithNoLocalData)
		}
	}

	if regionStats, ok := stats["regionStats"].(map[string]interface{}); ok {
		for region, stats := range regionStats {
			if regionStatsMap, ok := stats.(map[string]interface{}); ok {
				if localDataPercentage, ok := getFloat64(regionStatsMap, "localDataPercentage"); ok {
					regionDataLocalityPercentage.WithLabelValues(region).Set(localDataPercentage * 100)
				}
			}
		}
	}

	if nodeScores, ok := stats["nodeDataLocalityScores"].(map[string]interface{}); ok {
		nodeInfoMap := fetchNodeInformation()

		for nodeName, score := range nodeScores {
			if scoreValue, ok := score.(float64); ok {
				nodeInfo, exists := nodeInfoMap[nodeName]
				if exists {
					nodeDataLocalityScore.WithLabelValues(
						nodeName,
						nodeInfo.NodeType,
						nodeInfo.Region,
						nodeInfo.Zone,
					).Set(scoreValue)
				} else {
					nodeDataLocalityScore.WithLabelValues(
						nodeName,
						"unknown",
						"unknown",
						"unknown",
					).Set(scoreValue)
				}
			}
		}
	}

	decisionsURL := fmt.Sprintf("%s/recent-scheduling-decisions", schedulerURL)
	resp, err = http.Get(decisionsURL)
	if err != nil {
		log.Printf("Error fetching scheduling decisions: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error response from scheduler for decisions: %s", resp.Status)
		return
	}

	var decisions []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&decisions); err != nil {
		log.Printf("Error decoding scheduling decisions: %v", err)
		return
	}

	for _, decision := range decisions {
		podName, _ := decision["podName"].(string)
		podNamespace, _ := decision["podNamespace"].(string)
		nodeName, _ := decision["nodeName"].(string)
		nodeType, _ := decision["nodeType"].(string)
		nodeRegion, _ := decision["nodeRegion"].(string)
		nodeZone, _ := decision["nodeZone"].(string)

		if score, ok := getFloat64(decision, "dataLocalityScore"); ok {
			dataLocalityScore.WithLabelValues(
				podName,
				podNamespace,
				nodeName,
				nodeType,
				nodeRegion,
				nodeZone,
			).Set(score)
		}

		podPlacementByNodeType.WithLabelValues(
			nodeType,
			nodeRegion,
			nodeZone,
			podNamespace,
		).Inc()

		schedulingDecisions.Inc()
	}
}

func collectResourceUtilizationMetrics(client *kubernetes.Clientset) {
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error fetching nodes: %v", err)
		return
	}

	for _, node := range nodes.Items {
		fieldSelector := fmt.Sprintf("spec.nodeName=%s", node.Name)
		pods, err := client.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
			FieldSelector: fieldSelector,
		})
		if err != nil {
			log.Printf("Error fetching pods on node %s: %v", node.Name, err)
			continue
		}

		var cpuRequests, memoryRequests int64
		for _, pod := range pods.Items {
			if pod.Status.Phase != "Running" {
				continue
			}

			for _, container := range pod.Spec.Containers {
				if container.Resources.Requests.Cpu() != nil {
					cpuRequests += container.Resources.Requests.Cpu().MilliValue()
				}
				if container.Resources.Requests.Memory() != nil {
					memoryRequests += container.Resources.Requests.Memory().Value()
				}
			}
		}

		cpuCapacity := node.Status.Capacity.Cpu().MilliValue()
		memoryCapacity := node.Status.Capacity.Memory().Value()

		var cpuEfficiency, memoryEfficiency float64

		if cpuCapacity > 0 {
			cpuUtilization := float64(cpuRequests) / float64(cpuCapacity)
			if cpuUtilization <= 0.8 {
				cpuEfficiency = 100 * (1 - math.Abs(0.75-cpuUtilization)/0.75)
			} else {
				cpuEfficiency = 100 * (1 - (cpuUtilization-0.8)*5)
			}
			if cpuEfficiency < 0 {
				cpuEfficiency = 0
			} else if cpuEfficiency > 100 {
				cpuEfficiency = 100
			}
		}

		if memoryCapacity > 0 {
			memUtilization := float64(memoryRequests) / float64(memoryCapacity)
			if memUtilization <= 0.8 {
				memoryEfficiency = 100 * (1 - math.Abs(0.75-memUtilization)/0.75)
			} else {
				memoryEfficiency = 100 * (1 - (memUtilization-0.8)*5)
			}
			if memoryEfficiency < 0 {
				memoryEfficiency = 0
			} else if memoryEfficiency > 100 {
				memoryEfficiency = 100
			}
		}

		resourceEfficiency.WithLabelValues(node.Name, "cpu").Set(cpuEfficiency)
		resourceEfficiency.WithLabelValues(node.Name, "memory").Set(memoryEfficiency)
	}
}

func collectEdgeUtilizationMetrics(client *kubernetes.Clientset) {
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error fetching nodes: %v", err)
		return
	}

	nodesByRegion := make(map[string][]string)
	nodeTypeByName := make(map[string]string)

	for _, node := range nodes.Items {
		region := "unknown"
		if val, ok := node.Labels["topology.kubernetes.io/region"]; ok {
			region = val
		}

		nodeType := "unknown"
		if val, ok := node.Labels["node-capability/node-type"]; ok {
			nodeType = val
		}

		nodesByRegion[region] = append(nodesByRegion[region], node.Name)
		nodeTypeByName[node.Name] = nodeType
	}

	pods, err := client.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error fetching pods: %v", err)
		return
	}

	podsByRegionAndType := make(map[string]map[string]int)

	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}

		nodeName := pod.Spec.NodeName
		nodeType := nodeTypeByName[nodeName]

		var nodeRegion string
		for region, nodes := range nodesByRegion {
			if contains(nodes, nodeName) {
				nodeRegion = region
				break
			}
		}

		if nodeRegion == "" {
			nodeRegion = "unknown"
		}

		if _, ok := podsByRegionAndType[nodeRegion]; !ok {
			podsByRegionAndType[nodeRegion] = make(map[string]int)
		}

		podsByRegionAndType[nodeRegion][nodeType]++
	}

	// edge utilization ratio by region
	for region, typeCounts := range podsByRegionAndType {
		total := 0
		edgeCount := 0

		for nodeType, count := range typeCounts {
			total += count
			if nodeType == "edge" {
				edgeCount = count
			}
		}

		if total > 0 {
			edgeRatio := float64(edgeCount) / float64(total)
			regionEdgeUtilization.WithLabelValues(region).Set(edgeRatio)
		}
	}
}

func getFloat64(m map[string]interface{}, key string) (float64, bool) {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case float64:
			return v, true
		case int:
			return float64(v), true
		case int64:
			return float64(v), true
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}

func contains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return item == str
		}
	}
	return false
}

func fetchNodeInformation() map[string]NodeInfo {
	nodeInfoMap := make(map[string]NodeInfo)
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("Error creating in-cluster config: %v", err)
		return nodeInfoMap
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Printf("Error creating clientset: %v", err)
		return nodeInfoMap
	}

	nodes, err := clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error fetching nodes: %v", err)
		return nodeInfoMap
	}

	for _, node := range nodes.Items {
		nodeType := "unknown"
		if val, ok := node.Labels["node-capability/node-type"]; ok {
			nodeType = val
		}

		region := "unknown"
		if val, ok := node.Labels["topology.kubernetes.io/region"]; ok {
			region = val
		}

		zone := "unknown"
		if val, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
			zone = val
		}

		nodeInfoMap[node.Name] = NodeInfo{
			Name:     node.Name,
			NodeType: nodeType,
			Region:   region,
			Zone:     zone,
		}
	}

	return nodeInfoMap
}
