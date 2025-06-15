import datetime
import json
import logging
from pathlib import Path

logger = logging.getLogger("report-generator")

class ReportGenerator:
    
    def __init__(self, output_dir, run_id):
        self.output_dir = Path(output_dir)
        self.run_id = run_id
    
    def save_results(self, results):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.output_dir / f"benchmark_results_{self.run_id}_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(results, f, default=str, indent=2)
        
        logger.info(f"Saved benchmark results to {results_file}")
        
        self._save_summary_csv(results, timestamp)
        self._save_metrics_csv(results, timestamp)
    
    def _save_summary_csv(self, results, timestamp):
        summary_file = self.output_dir / f"benchmark_summary_{self.run_id}_{timestamp}.csv"
        
        with open(summary_file, 'w') as f:
            f.write("workload,scheduler,iteration,data_locality_score,weighted_locality_score,size_weighted_score,local_data_percentage,cross_region_percentage,avg_placement_latency,edge_placements,cloud_placements,total_placements\n")
            
            for workload_key, metrics in results["metrics"].items():
                parts = workload_key.rsplit('_', 2)
                if len(parts) >= 3:
                    workload, scheduler, iteration = parts
                    
                    dl_metrics = metrics.get("data_locality_metrics", {})
                    data_locality_score = dl_metrics.get("overall_score", 0)
                    weighted_score = dl_metrics.get("weighted_score", 0)
                    size_weighted_score = dl_metrics.get("size_weighted_score", 0)
                    
                    total_data_size = dl_metrics.get("total_data_size", 0)
                    local_percentage = 0
                    cross_region_percentage = 0
                    if total_data_size > 0:
                        local_percentage = (dl_metrics.get("local_data_size", 0) / total_data_size) * 100
                        cross_region_percentage = (dl_metrics.get("cross_region_data_size", 0) / total_data_size) * 100
                    
                    sched_metrics = metrics.get("scheduling_metrics", {})
                    avg_placement_latency = sched_metrics.get("avg_placement_latency", 0)
                    
                    edge_placements = sum(1 for pod in metrics.get("pod_metrics", []) if pod.get("node_type") == "edge")
                    cloud_placements = sum(1 for pod in metrics.get("pod_metrics", []) if pod.get("node_type") == "cloud")
                    total_placements = len(metrics.get("pod_metrics", []))
                    
                    f.write(f"{workload},{scheduler},{iteration},{data_locality_score:.4f},{weighted_score:.4f},{size_weighted_score:.4f},{local_percentage:.2f},{cross_region_percentage:.2f},{avg_placement_latency:.4f},{edge_placements},{cloud_placements},{total_placements}\n")
        
        logger.info(f"Saved benchmark summary to {summary_file}")
    
    def _save_metrics_csv(self, results, timestamp):
        metrics_file = self.output_dir / f"detailed_metrics_{self.run_id}_{timestamp}.csv"
        
        with open(metrics_file, 'w') as f:
            f.write("workload,scheduler,iteration,pod_name,node_name,node_type,node_region,placement_latency,local_refs,same_zone_refs,same_region_refs,cross_region_refs,total_refs,local_data_size,total_data_size\n")
            
            for workload_key, metrics in results["metrics"].items():
                parts = workload_key.rsplit('_', 2)
                if len(parts) >= 3:
                    workload, scheduler, iteration = parts
                    
                    for pod in metrics.get("pod_metrics", []):
                        pod_name = pod.get("pod_name", "")
                        node_name = pod.get("node", "")
                        node_type = pod.get("node_type", "")
                        node_region = pod.get("node_region", "")
                        placement_latency = pod.get("placement_latency", 0)
                        
                        # we extract pod-level data locality if available
                        pod_locality = pod.get("data_locality", {})
                        local_refs = pod_locality.get("local_refs", 0)
                        same_zone_refs = pod_locality.get("same_zone_refs", 0)
                        same_region_refs = pod_locality.get("same_region_refs", 0)
                        cross_region_refs = pod_locality.get("cross_region_refs", 0)
                        total_refs = pod_locality.get("total_refs", 0)
                        local_data_size = pod_locality.get("local_data_size", 0)
                        total_data_size = pod_locality.get("total_data_size", 0)
                        
                        f.write(f"{workload},{scheduler},{iteration},{pod_name},{node_name},{node_type},{node_region},{placement_latency:.4f},{local_refs},{same_zone_refs},{same_region_refs},{cross_region_refs},{total_refs},{local_data_size},{total_data_size}\n")
        
        logger.info(f"Saved detailed metrics to {metrics_file}")
    
    def generate_report(self, results):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"benchmark_report_{self.run_id}_{timestamp}.md"
        
        with open(report_file, 'w') as f:
            self._write_report_header(f, results)
            self._write_experimental_setup(f, results["metadata"])
            self._write_performance_summary(f, results["comparison"])
            self._write_data_locality_analysis(f, results["comparison"])
            self._write_scheduling_performance(f, results["comparison"])
            self._write_network_efficiency_analysis(f, results["comparison"])
            self._write_workload_specific_results(f, results["comparison"])
            self._write_statistical_summary(f, results["comparison"])
        
        logger.info(f"Generated benchmark report: {report_file}")
        return report_file
    
    def _write_report_header(self, f, results):
        f.write("# Data Locality Scheduler Performance Evaluation\n\n")
        f.write(f"**Report ID:** {self.run_id}\n")
        f.write(f"**Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Duration:** {results['metadata'].get('benchmark_duration', 0):.2f} seconds\n\n")
    
    def _write_experimental_setup(self, f, metadata):
        f.write("## Experimental Setup\n\n")
        
        cluster_info = metadata.get("cluster_info", {})
        f.write("### Cluster Configuration\n\n")
        f.write(f"| Parameter | Value |\n")
        f.write(f"|-----------|-------|\n")
        f.write(f"| Total Nodes | {cluster_info.get('node_count', 0)} |\n")
        f.write(f"| Edge Nodes | {len(cluster_info.get('node_types', {}).get('edge', []))} |\n")
        f.write(f"| Cloud Nodes | {len(cluster_info.get('node_types', {}).get('cloud', []))} |\n")
        f.write(f"| Other Nodes | {len(cluster_info.get('node_types', {}).get('other', []))} |\n\n")
        
        config = metadata.get("config", {})
        schedulers = config.get("schedulers", [])
        f.write("### Schedulers Tested\n\n")
        for scheduler in schedulers:
            if isinstance(scheduler, dict):
                name = scheduler.get("name", "unknown")
                description = scheduler.get("description", "")
                f.write(f"- **{name}**: {description}\n")
            else:
                f.write(f"- **{scheduler}**\n")
        f.write("\n")
        
        workloads = config.get("workloads", [])
        f.write("### Workloads Evaluated\n\n")
        f.write("| Workload | Iterations | Data Intensity | Compute Intensity |\n")
        f.write("|----------|------------|----------------|-------------------|\n")
        for workload in workloads:
            if isinstance(workload, dict):
                name = workload.get("name", "unknown")
                iterations = workload.get("iterations", 1)
                data_intensity = workload.get("data_intensity", "unknown")
                compute_intensity = workload.get("compute_intensity", "unknown")
                f.write(f"| {name} | {iterations} | {data_intensity} | {compute_intensity} |\n")
            else:
                f.write(f"| {workload} | 1 | unknown | unknown |\n")
        f.write("\n")
    
    def _write_performance_summary(self, f, comparison):
        f.write("## Performance Summary\n\n")
        
        overall_averages = comparison.get("overall_averages", {})
        
        f.write("### Key Performance Indicators\n\n")
        f.write("| Metric | Improvement (%) |\n")
        f.write("|--------|------------------|\n")
        
        metrics_map = {
            "data_locality_improvement": "Data Locality Score",
            "size_weighted_improvement": "Size-Weighted Locality",
            "local_data_improvement": "Local Data Access",
            "cross_region_reduction": "Cross-Region Transfer Reduction",
            "network_overhead_reduction": "Network Overhead Reduction",
            "processing_overhead_reduction": "Processing Overhead Reduction"
        }
        
        for key, label in metrics_map.items():
            value = overall_averages.get(key, 0)
            f.write(f"| {label} | {value:.2f} |\n")
        
        f.write("\n")
    
    def _write_data_locality_analysis(self, f, comparison):
        f.write("## Data Locality Analysis\n\n")
        
        f.write("### Data Locality Scores by Workload\n\n")
        f.write("| Workload | Default Scheduler | Data Locality Scheduler | Improvement (%) |\n")
        f.write("|----------|-------------------|------------------------|------------------|\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'data_locality_comparison' in workload_comparison:
                    data_comp = workload_comparison['data_locality_comparison']
                    
                    default_score = data_comp.get('default-scheduler', {}).get('mean', 0)
                    dl_score = data_comp.get('data-locality-scheduler', {}).get('mean', 0)
                    improvement = data_comp.get('improvement_percentage', 0)
                    
                    f.write(f"| {workload} | {default_score:.4f} | {dl_score:.4f} | {improvement:.2f} |\n")
        
        f.write("\n")
        
        f.write("### Data Transfer Breakdown\n\n")
        f.write("| Workload | Scheduler | Local Data (%) | Cross Region (%) |\n")
        f.write("|----------|-----------|----------------|------------------|\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'data_locality_comparison' in workload_comparison:
                    data_comp = workload_comparison['data_locality_comparison']
                    
                    for scheduler in ['default-scheduler', 'data-locality-scheduler']:
                        if scheduler in data_comp:
                            sched_data = data_comp[scheduler]
                            local_pct = sched_data.get('local_data_percentage', 0)
                            cross_region_pct = sched_data.get('cross_region_percentage', 0)
                            
                            f.write(f"| {workload} | {scheduler} | {local_pct:.1f} | {cross_region_pct:.1f} |\n")
        
        f.write("\n")
    
    def _write_scheduling_performance(self, f, comparison):
        f.write("## Scheduling Performance\n\n")
        
        f.write("### Placement Latency Analysis\n\n")
        f.write("| Workload | Scheduler | Mean Latency (s) | Min Latency (s) | Max Latency (s) | Std Dev (s) |\n")
        f.write("|----------|-----------|------------------|-----------------|-----------------|-------------|\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'scheduling_latency_comparison' in workload_comparison:
                    latency_comp = workload_comparison['scheduling_latency_comparison']
                    
                    for scheduler, latency_data in latency_comp.items():
                        if isinstance(latency_data, dict) and 'mean' in latency_data:
                            mean_latency = latency_data.get('mean', 0)
                            min_latency = latency_data.get('min', 0)
                            max_latency = latency_data.get('max', 0)
                            
                            # standard deviation
                            latencies = latency_data.get('latencies', [])
                            std_dev = 0
                            if len(latencies) > 1:
                                variance = sum((x - mean_latency) ** 2 for x in latencies) / len(latencies)
                                std_dev = variance ** 0.5
                            
                            f.write(f"| {workload} | {scheduler} | {mean_latency:.4f} | {min_latency:.4f} | {max_latency:.4f} | {std_dev:.4f} |\n")
        
        f.write("\n")
        
        f.write("### Node Distribution Analysis\n\n")
        f.write("| Workload | Scheduler | Edge Placements | Cloud Placements | Total Placements | Edge Utilization (%) |\n")
        f.write("|----------|-----------|-----------------|-------------------|------------------|-----------------------|\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'node_distribution_comparison' in workload_comparison:
                    node_comp = workload_comparison['node_distribution_comparison']
                    
                    for scheduler, node_data in node_comp.items():
                        if isinstance(node_data, dict):
                            edge_placements = node_data.get('edge_placements', 0)
                            cloud_placements = node_data.get('cloud_placements', 0)
                            total_placements = node_data.get('total_placements', 0)
                            edge_percentage = node_data.get('edge_percentage', 0)
                            
                            f.write(f"| {workload} | {scheduler} | {edge_placements} | {cloud_placements} | {total_placements} | {edge_percentage:.1f} |\n")
        
        f.write("\n")
    
    def _write_network_efficiency_analysis(self, f, comparison):
        f.write("## Network Efficiency Analysis\n\n")
        
        f.write("### Data Transfer Volume Comparison\n\n")
        f.write("| Workload | Scheduler | Total Data (MB) | Local Access (MB) | Remote Transfer (MB) | Transfer Reduction (%) |\n")
        f.write("|----------|-----------|-----------------|-------------------|----------------------|------------------------|\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'network_comparison' in workload_comparison:
                    network_comp = workload_comparison['network_comparison']
                    
                    for scheduler, network_data in network_comp.items():
                        if isinstance(network_data, dict):
                            total_mb = network_data.get('total_data_mb', 0)
                            local_mb = network_data.get('local_data_mb', 0)
                            remote_mb = total_mb - local_mb
                            
                            # calculate transfer reduction if both schedulers present
                            transfer_reduction = 0
                            if 'transfer_reduction_percentage' in network_comp:
                                transfer_reduction = network_comp['transfer_reduction_percentage']
                            
                            f.write(f"| {workload} | {scheduler} | {total_mb:.2f} | {local_mb:.2f} | {remote_mb:.2f} | {transfer_reduction:.2f} |\n")
        
        f.write("\n")
    
    def _write_workload_specific_results(self, f, comparison):
        f.write("## Workload-Specific Results\n\n")
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                f.write(f"### {workload}\n\n")
                
                f.write("| Metric | Default Scheduler | Data Locality Scheduler | Delta | Improvement (%) |\n")
                f.write("|--------|-------------------|------------------------|-------|------------------|\n")
                
                # data locality metrics
                if 'data_locality_comparison' in workload_comparison:
                    data_comp = workload_comparison['data_locality_comparison']
                    default_score = data_comp.get('default-scheduler', {}).get('mean', 0)
                    dl_score = data_comp.get('data-locality-scheduler', {}).get('mean', 0)
                    delta = dl_score - default_score
                    improvement = data_comp.get('improvement_percentage', 0)
                    
                    f.write(f"| Data Locality Score | {default_score:.4f} | {dl_score:.4f} | {delta:+.4f} | {improvement:.2f} |\n")
                
                # scheduling latency
                if 'scheduling_latency_comparison' in workload_comparison:
                    latency_comp = workload_comparison['scheduling_latency_comparison']
                    default_latency = latency_comp.get('default-scheduler', {}).get('mean', 0)
                    dl_latency = latency_comp.get('data-locality-scheduler', {}).get('mean', 0)
                    latency_delta = dl_latency - default_latency
                    latency_improvement = latency_comp.get('improvement_percentage', 0)
                    
                    f.write(f"| Avg Placement Latency (s) | {default_latency:.4f} | {dl_latency:.4f} | {latency_delta:+.4f} | {latency_improvement:.2f} |\n")
                
                f.write("\n")
    
    def _write_statistical_summary(self, f, comparison):
        f.write("## Statistical Summary\n\n")
        
        overall_averages = comparison.get("overall_averages", {})
        
        f.write("### Aggregate Performance Improvements\n\n")
        f.write("| Category | Mean Improvement (%) | Sample Size |\n")
        f.write("|----------|----------------------|-------------|\n")
        
        # how many workloads contributed to each metric
        workload_count = len([w for w in comparison.keys() if w != "overall_averages"])
        
        improvement_categories = {
            "data_locality_improvement": "Data Locality",
            "size_weighted_improvement": "Size-Weighted Locality", 
            "local_data_improvement": "Local Data Access",
            "cross_region_reduction": "Cross-Region Transfer Reduction"
        }
        
        for key, label in improvement_categories.items():
            improvement = overall_averages.get(key, 0)
            f.write(f"| {label} | {improvement:.2f} | {workload_count} |\n")
        
        f.write("\n")
        
        f.write("### Performance Variance Analysis\n\n")
        
        # Calculate variance across workloads for key metrics
        locality_scores = []
        latency_improvements = []
        
        for workload, workload_comparison in comparison.items():
            if isinstance(workload_comparison, dict) and workload != "overall_averages":
                if 'data_locality_comparison' in workload_comparison:
                    improvement = workload_comparison['data_locality_comparison'].get('improvement_percentage', 0)
                    locality_scores.append(improvement)
                
                if 'scheduling_latency_comparison' in workload_comparison:
                    latency_improvement = workload_comparison['scheduling_latency_comparison'].get('improvement_percentage', 0)
                    latency_improvements.append(latency_improvement)
        
        f.write("| Metric | Min (%) | Max (%) | Range (%) | Std Dev (%) |\n")
        f.write("|--------|---------|---------|-----------|-------------|\n")
        
        if locality_scores:
            min_locality = min(locality_scores)
            max_locality = max(locality_scores)
            range_locality = max_locality - min_locality
            
            mean_locality = sum(locality_scores) / len(locality_scores)
            variance_locality = sum((x - mean_locality) ** 2 for x in locality_scores) / len(locality_scores)
            std_dev_locality = variance_locality ** 0.5
            
            f.write(f"| Data Locality Improvement | {min_locality:.2f} | {max_locality:.2f} | {range_locality:.2f} | {std_dev_locality:.2f} |\n")
        
        if latency_improvements:
            min_latency = min(latency_improvements)
            max_latency = max(latency_improvements)
            range_latency = max_latency - min_latency
            
            mean_latency = sum(latency_improvements) / len(latency_improvements)
            variance_latency = sum((x - mean_latency) ** 2 for x in latency_improvements) / len(latency_improvements)
            std_dev_latency = variance_latency ** 0.5
            
            f.write(f"| Scheduling Latency Improvement | {min_latency:.2f} | {max_latency:.2f} | {range_latency:.2f} | {std_dev_latency:.2f} |\n")
        
        f.write("\n")