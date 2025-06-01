import argparse
import datetime
import json
import logging
import uuid
from pathlib import Path

from configuration_manager import ConfigurationManager
from cluster_manager import ClusterManager
from storage_manager import StorageManager
from workload_executor import WorkloadExecutor
from metrics_collector import MetricsCollector
from results_comparator import ResultsComparator
from report_generator import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("benchmarks/results/benchmark_run.log")
    ]
)
logger = logging.getLogger("benchmark-runner")

class BenchmarkRunner:
    
    def __init__(self, config_file, output_dir, run_id=None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id or str(uuid.uuid4())[:8]
        
        self.config_manager = ConfigurationManager(config_file)
        self.cluster_manager = ClusterManager()
        self.storage_manager = StorageManager(self.config_manager, self.cluster_manager)
        self.workload_executor = WorkloadExecutor(self.config_manager, self.cluster_manager)
        self.metrics_collector = MetricsCollector(self.cluster_manager, self.storage_manager)
        self.results_comparator = ResultsComparator()
        self.report_generator = ReportGenerator(self.output_dir, self.run_id)
        
        self.results = {
            "metadata": {
                "run_id": self.run_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "cluster_info": {},
                "config": self.config_manager.get_config()
            },
            "workloads": {},
            "metrics": {},
            "comparison": {}
        }
    
    def prepare_environment(self):
        logger.info("Preparing benchmark environment")
        
        self.cluster_manager.ensure_namespace(self.config_manager.get_namespace())
        self.cluster_manager.collect_cluster_info(self.results["metadata"]["cluster_info"])
        
        if self.config_manager.should_setup_storage():
            self.storage_manager.deploy_storage_services()
            self.storage_manager.wait_for_readiness()
            self.storage_manager.initialize_data()
    
    def run_workload(self, workload_name, scheduler_name, iteration=1):
        logger.info(f"Running workload '{workload_name}' with scheduler '{scheduler_name}' (iteration {iteration})")
        
        workload_key = f"{workload_name}_{scheduler_name}_{iteration}"
        
        success = self.workload_executor.execute_workload(
            workload_name, scheduler_name, iteration, self.run_id
        )
        
        if success:
            execution_results = self.workload_executor.get_execution_results(workload_key)
            self.results["workloads"][workload_key] = execution_results
            
            metrics = self.metrics_collector.collect_workload_metrics(
                workload_name, scheduler_name, iteration, self.run_id
            )
            self.results["metrics"][workload_key] = metrics
        
        return success
    
    def run_benchmarks(self):
        logger.info("Starting benchmark runs")
        
        benchmark_start_time = datetime.datetime.now()
        success = True
        
        try:
            self.prepare_environment()
            
            for workload_config in self.config_manager.get_workloads():
                workload_name = self._extract_workload_name(workload_config)
                iterations = self._extract_iterations(workload_config)
                
                for scheduler in self.config_manager.get_schedulers():
                    scheduler_name = self._extract_scheduler_name(scheduler)
                    
                    for iteration in range(1, iterations + 1):
                        try:
                            workload_success = self.run_workload(workload_name, scheduler_name, iteration)
                            if not workload_success:
                                logger.warning(f"Workload {workload_name} with scheduler {scheduler_name} (iteration {iteration}) failed")
                                success = False
                        except Exception as e:
                            logger.error(f"Error running workload {workload_name} with scheduler {scheduler_name} (iteration {iteration}): {e}")
                            success = False
            
            self._finalize_results(benchmark_start_time, success)
            
        except Exception as e:
            logger.error(f"Benchmark run encountered an error: {e}")
            self._finalize_results(benchmark_start_time, False)
            success = False
        
        return success
    
    def _extract_workload_name(self, workload_config):
        return workload_config.get('name') if isinstance(workload_config, dict) else workload_config
    
    def _extract_iterations(self, workload_config):
        return workload_config.get('iterations', 3) if isinstance(workload_config, dict) else 3
    
    def _extract_scheduler_name(self, scheduler):
        return scheduler.get('name') if isinstance(scheduler, dict) else scheduler
    
    def _finalize_results(self, start_time, success):
        end_time = datetime.datetime.now()
        self.results["metadata"]["benchmark_duration"] = (end_time - start_time).total_seconds()
        self.results["metadata"]["completed"] = success
        
        try:
            comparison_results = self.results_comparator.compare_results(self.results["metrics"])
            self.results["comparison"] = comparison_results
        except Exception as e:
            logger.error(f"Error comparing results: {e}")
        
        self.report_generator.save_results(self.results)
        self.report_generator.generate_report(self.results)

def main():
    parser = argparse.ArgumentParser(description='Run scheduler benchmarks')
    parser.add_argument('--config', type=str, default='benchmarks/simulated/framework/benchmark_config.yaml')
    parser.add_argument('--output-dir', type=str, default='benchmarks/simulated/results')
    parser.add_argument('--run-id', type=str, default=None)
    
    args = parser.parse_args()
    
    runner = BenchmarkRunner(args.config, args.output_dir, args.run_id)
    runner.run_benchmarks()

if __name__ == '__main__':
    main()