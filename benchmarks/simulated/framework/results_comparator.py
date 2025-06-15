import logging

logger = logging.getLogger("results-comparator")

class ResultsComparator:
    
    def compare_results(self, metrics_data):
        logger.info("Comparing results between schedulers")
        
        schedulers, workloads = self._extract_schedulers_and_workloads(metrics_data)
        
        comparison = {}
        for workload in workloads:
            comparison[workload] = self._compare_workload_metrics(
                workload, schedulers, metrics_data
            )
        
        comparison["overall_averages"] = self._calculate_overall_averages(comparison)
        
        return comparison
    
    def _extract_schedulers_and_workloads(self, metrics_data):
        schedulers = set()
        workloads = set()
        
        for workload_key in metrics_data:
            parts = workload_key.rsplit('_', 2)
            if len(parts) >= 3:
                workload = parts[0]
                scheduler = parts[1]
                workloads.add(workload)
                schedulers.add(scheduler)
        
        return schedulers, workloads
    
    def _compare_workload_metrics(self, workload, schedulers, metrics_data):
        scheduler_metrics = self._aggregate_scheduler_metrics(
            workload, schedulers, metrics_data
        )
        
        comparison = {}
        comparison["data_locality_comparison"] = self._compare_data_locality(scheduler_metrics)
        comparison["preference_comparison"] = self._compare_preferences(scheduler_metrics)
        comparison["network_comparison"] = self._compare_network_metrics(scheduler_metrics)
        comparison["scheduling_latency_comparison"] = self._compare_scheduling_latency(scheduler_metrics)
        comparison["node_distribution_comparison"] = self._compare_node_distribution(scheduler_metrics)
        
        return comparison
    
    def _aggregate_scheduler_metrics(self, workload, schedulers, metrics_data):
        scheduler_metrics = {}
        
        for scheduler in schedulers:
            scheduler_metrics[scheduler] = {
                "data_locality_scores": [],
                "weighted_locality_scores": [],
                "size_weighted_scores": [],
                "local_data_percentages": [],
                "cross_region_percentages": [],
                "placement_latencies": [],
                "edge_placements": 0,
                "cloud_placements": 0,
                "total_placements": 0,
                "edge_preference_satisfaction": [],
                "cloud_preference_satisfaction": [],
                "region_preference_satisfaction": []
            }
            
            for iteration in range(1, 10):
                workload_key = f"{workload}_{scheduler}_{iteration}"
                if workload_key in metrics_data:
                    self._process_workload_iteration(
                        metrics_data[workload_key], scheduler_metrics[scheduler]
                    )
        
        return scheduler_metrics
    
    def _process_workload_iteration(self, metrics, scheduler_data):
        if "data_locality_metrics" in metrics:
            locality_metrics = metrics["data_locality_metrics"]
            
            scheduler_data["data_locality_scores"].append(
                locality_metrics.get("overall_score", 0)
            )
            scheduler_data["weighted_locality_scores"].append(
                locality_metrics.get("weighted_score", 0)
            )
            scheduler_data["size_weighted_scores"].append(
                locality_metrics.get("size_weighted_score", 0)
            )
            
            total_data_size = locality_metrics.get("total_data_size", 0)
            if total_data_size > 0:
                local_pct = (locality_metrics.get("local_data_size", 0) / total_data_size) * 100
                cross_region_pct = (locality_metrics.get("cross_region_data_size", 0) / total_data_size) * 100
                
                scheduler_data["local_data_percentages"].append(local_pct)
                scheduler_data["cross_region_percentages"].append(cross_region_pct)
        
        if "scheduling_metrics" in metrics and "placement_latencies" in metrics["scheduling_metrics"]:
            scheduler_data["placement_latencies"].extend(
                metrics["scheduling_metrics"]["placement_latencies"]
            )
        
        for pod in metrics.get("pod_metrics", []):
            scheduler_data["total_placements"] += 1
            
            if pod.get("node_type") == "edge":
                scheduler_data["edge_placements"] += 1
            elif pod.get("node_type") == "cloud":
                scheduler_data["cloud_placements"] += 1
            
            self._process_preference_satisfaction(pod, scheduler_data)
    
    def _process_preference_satisfaction(self, pod, scheduler_data):
        annotations = pod.get("data_annotations", {})
        
        if "scheduler.thesis/prefer-edge" in annotations:
            edge_satisfied = pod.get("node_type") == "edge"
            scheduler_data["edge_preference_satisfaction"].append(1.0 if edge_satisfied else 0.0)
        
        if "scheduler.thesis/prefer-cloud" in annotations:
            cloud_satisfied = pod.get("node_type") == "cloud"
            scheduler_data["cloud_preference_satisfaction"].append(1.0 if cloud_satisfied else 0.0)
        
        if "scheduler.thesis/prefer-region" in annotations:
            preferred_region = annotations.get("scheduler.thesis/prefer-region")
            pod_region = pod.get("node_region", "")
            region_satisfied = preferred_region == pod_region
            scheduler_data["region_preference_satisfaction"].append(1.0 if region_satisfied else 0.0)
    
    def _compare_data_locality(self, scheduler_metrics):
        comparison = {}
        
        for scheduler, metrics in scheduler_metrics.items():
            if metrics["data_locality_scores"]:
                avg_score = sum(metrics["data_locality_scores"]) / len(metrics["data_locality_scores"])
                avg_weighted = sum(metrics["weighted_locality_scores"]) / len(metrics["weighted_locality_scores"])
                avg_size_weighted = sum(metrics["size_weighted_scores"]) / len(metrics["size_weighted_scores"])
                
                comparison[scheduler] = {
                    "mean": avg_score,
                    "weighted_mean": avg_weighted,
                    "size_weighted_mean": avg_size_weighted,
                    "scores": metrics["data_locality_scores"],
                    "min": min(metrics["data_locality_scores"]),
                    "max": max(metrics["data_locality_scores"]),
                    "local_data_percentage": self._safe_average(metrics["local_data_percentages"]),
                    "cross_region_percentage": self._safe_average(metrics["cross_region_percentages"])
                }
        
        self._add_improvement_calculations(comparison)
        return comparison
    
    def _compare_preferences(self, scheduler_metrics):
        comparison = {}
        
        for scheduler, metrics in scheduler_metrics.items():
            comparison[scheduler] = {
                "edge_preference_satisfaction": self._safe_average(metrics["edge_preference_satisfaction"]),
                "cloud_preference_satisfaction": self._safe_average(metrics["cloud_preference_satisfaction"]),
                "region_preference_satisfaction": self._safe_average(metrics["region_preference_satisfaction"])
            }
        
        return comparison
    
    def _compare_network_metrics(self, scheduler_metrics):
        return {}
    
    def _compare_scheduling_latency(self, scheduler_metrics):
        comparison = {}
        
        for scheduler, metrics in scheduler_metrics.items():
            if metrics["placement_latencies"]:
                latencies = metrics["placement_latencies"]
                comparison[scheduler] = {
                    "mean": sum(latencies) / len(latencies),
                    "min": min(latencies),
                    "max": max(latencies),
                    "latencies": latencies
                }
        
        return comparison
    
    def _compare_node_distribution(self, scheduler_metrics):
        comparison = {}
        
        for scheduler, metrics in scheduler_metrics.items():
            total_placements = metrics["total_placements"]
            if total_placements > 0:
                edge_percentage = (metrics["edge_placements"] / total_placements) * 100
                cloud_percentage = (metrics["cloud_placements"] / total_placements) * 100
                
                comparison[scheduler] = {
                    "edge_placements": metrics["edge_placements"],
                    "cloud_placements": metrics["cloud_placements"],
                    "total_placements": total_placements,
                    "edge_percentage": edge_percentage,
                    "cloud_percentage": cloud_percentage
                }
        
        return comparison
    
    def _add_improvement_calculations(self, comparison):
        if "data-locality-scheduler" in comparison and "default-scheduler" in comparison:
            baseline = comparison["default-scheduler"]["mean"]
            new_score = comparison["data-locality-scheduler"]["mean"]
            
            if baseline > 0:
                improvement = ((new_score - baseline) / baseline) * 100
                comparison["improvement_percentage"] = improvement
            else:
                comparison["improvement_percentage"] = new_score * 100 if new_score > 0 else 0
    
    def _calculate_overall_averages(self, comparison):
        improvements = {
            "data_locality_improvement": [],
            "size_weighted_improvement": [],
            "local_data_improvement": [],
            "cross_region_reduction": [],
            "network_overhead_reduction": [],
            "processing_overhead_reduction": []
        }
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict):
                if "data_locality_comparison" in workload_comparison:
                    data_comp = workload_comparison["data_locality_comparison"]
                    if "improvement_percentage" in data_comp:
                        improvements["data_locality_improvement"].append(data_comp["improvement_percentage"])
        
        averages = {}
        for key, values in improvements.items():
            averages[key] = sum(values) / len(values) if values else 0.0
        
        return averages
    
    def _safe_average(self, values):
        return sum(values) / len(values) if values else 0