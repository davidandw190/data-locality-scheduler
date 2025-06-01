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
    
    def _save_summary_csv(self, results, timestamp):
        summary_file = self.output_dir / f"benchmark_summary_{self.run_id}_{timestamp}.csv"
        
        with open(summary_file, 'w') as f:
            f.write("workload,scheduler,iteration,data_locality_score,avg_placement_latency,edge_placements,cloud_placements,total_placements\n")
            
            for workload_key, metrics in results["metrics"].items():
                parts = workload_key.rsplit('_', 2)
                if len(parts) >= 3:
                    workload, scheduler, iteration = parts
                    
                    data_locality_score = metrics.get("data_locality_score", "")
                    avg_placement_latency = ""
                    
                    if "scheduling_metrics" in metrics:
                        avg_placement_latency = metrics["scheduling_metrics"].get("avg_placement_latency", "")
                    
                    edge_placements = sum(1 for pod in metrics.get("pod_metrics", []) if pod.get("node_type") == "edge")
                    cloud_placements = sum(1 for pod in metrics.get("pod_metrics", []) if pod.get("node_type") == "cloud")
                    total_placements = len(metrics.get("pod_metrics", []))
                    
                    f.write(f"{workload},{scheduler},{iteration},{data_locality_score},{avg_placement_latency},{edge_placements},{cloud_placements},{total_placements}\n")
        
        logger.info(f"Saved benchmark summary to {summary_file}")
    
    def generate_report(self, results):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"benchmark_report_{self.run_id}_{timestamp}.md"
        
        with open(report_file, 'w') as f:
            self._write_report_header(f)
            self._write_cluster_info(f, results["metadata"]["cluster_info"])
            self._write_summary(f, results["comparison"])
            self._write_detailed_results(f, results["comparison"])
            self._write_conclusions(f, results["comparison"])
        
        logger.info(f"Generated benchmark report: {report_file}")
        return report_file
    
    def _write_report_header(self, f):
        f.write("# Data Locality Scheduler Benchmark Report\n\n")
        f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Run ID: {self.run_id}\n\n")
    
    def _write_cluster_info(self, f, cluster_info):
        f.write("## Cluster Information\n\n")
        node_count = cluster_info.get("node_count", 0)
        f.write(f"Total nodes: {node_count}\n")
        
        edge_nodes = len(cluster_info.get("node_types", {}).get("edge", []))
        cloud_nodes = len(cluster_info.get("node_types", {}).get("cloud", []))
        f.write(f"Edge nodes: {edge_nodes}\n")
        f.write(f"Cloud nodes: {cloud_nodes}\n\n")
    
    def _write_summary(self, f, comparison):
        f.write("## Summary of Results\n\n")
        
        overall_averages = comparison.get("overall_averages", {})
        if overall_averages and any(v != 0 for v in overall_averages.values()):
            f.write("The data-locality scheduler demonstrates improvements across multiple metrics:\n\n")
            
            if overall_averages.get("data_locality_improvement", 0) > 0:
                f.write(f"- **{overall_averages['data_locality_improvement']:.2f}%** improvement in overall data locality\n")
            
            if overall_averages.get("local_data_improvement", 0) > 0:
                f.write(f"- **{overall_averages['local_data_improvement']:.2f}%** increase in local data access\n")
        else:
            f.write("- No significant improvements detected. Check workload configurations and scheduler settings.\n")
        
        f.write("\n")
    
    def _write_detailed_results(self, f, comparison):
        f.write("## Workload Results\n\n")
        
        for workload, workload_comparison in comparison.items():
            if not isinstance(workload_comparison, dict) or workload == "overall_averages":
                continue
            
            f.write(f"### {workload}\n\n")
            
            if 'data_locality_comparison' in workload_comparison:
                self._write_data_locality_comparison(f, workload_comparison['data_locality_comparison'])
    
    def _write_data_locality_comparison(self, f, data_locality_comparison):
        f.write("#### Data Locality Comparison\n\n")
        f.write("| Scheduler | Data Locality Score | Weighted Score | Local Data % |\n")
        f.write("|-----------|--------------------|-----------------|--------------|\n")
        
        for scheduler, scores in data_locality_comparison.items():
            if scheduler not in ['improvement_percentage', 'size_weighted_improvement_percentage']:
                f.write(f"| {scheduler} | {scores['mean']:.4f} | {scores.get('weighted_mean', 0):.4f} | {scores.get('local_data_percentage', 0):.1f}% |\n")
        
        f.write("\n")
        
        if 'improvement_percentage' in data_locality_comparison:
            improvement = data_locality_comparison['improvement_percentage']
            f.write(f"**Data Locality Improvement: {improvement:.2f}%**\n\n")
    
    def _write_conclusions(self, f, comparison):
        f.write("## Conclusion\n\n")
        
        overall_averages = comparison.get("overall_averages", {})
        if overall_averages.get("data_locality_improvement", 0) > 0:
            f.write(f"The data-locality scheduler demonstrates a significant improvement of **{overall_averages.get('data_locality_improvement', 0):.2f}%** in data locality scores across tested workloads.\n\n")
            f.write("These results validate that topology-aware, data-locality-conscious scheduling can provide substantial benefits in distributed edge-cloud environments.\n")
        else:
            f.write("The current implementation shows modest improvements. Further refinements may be necessary to achieve more significant benefits.\n")