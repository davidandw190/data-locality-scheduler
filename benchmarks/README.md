#  Benchmarking Framework

> **Bachelor's Thesis Project Component**  
> *Data-Locality-Aware Scheduling for Serverless Containerized Workflows across the Edge-Cloud Continuum*  
> **Author:** Andrei-David Nan  
> **Supervisor:** Lect. Dr. Adrian Spătaru  
> **Institution:** West University of Timișoara, Faculty of Mathematics and Computer Science

## Overview

This benchmarking framework provides automated evaluation capabilities for comparing configurations of our data-locality-aware scheduler  against the default Kubernetes scheduler.

The benchmarker was specifically designed to evaluate the data-locality-aware scheduler developed as part of this bachelor's thesis, which uses Multi-Criteria Decision Making (MCDM) algorithms to optimize workload placement based on data proximity, resource availability, and network topology.

## Framework Architecture

The benchmarker consists of nine main components that manage the evaluation lifecycle:

### Core Components

- **Benchmark Runner** - Orchestrates evaluation phases and manages scheduler alternation
- **Configuration Manager** - Centralized control via YAML-based specifications
- **Cluster Manager** - Kubernetes API abstraction and namespace management
- **Storage Manager** - MinIO deployment and distributed storage configuration
- **Data Initializer** - Test dataset population with controlled placement strategies
- **Workload Executor** - Dynamic workload deployment and monitoring
- **Metrics Collector** - Performance data capture and network transfer measurement
- **Results Comparator** - Statistical analysis and scheduler comparison
- **Visualization Module** - Report generation and performance charts

## Project Structure

```bash
benchmarks/
├── simulated/
│   ├── framework/
│   │   ├── benchmark_runner.py        # Main orchestration component
│   │   ├── configuration_manager.py   # Config file management
│   │   ├── cluster_manager.py         # Kubernetes API interactions
│   │   ├── storage_manager.py         # MinIO storage deployment
│   │   ├── workload_executor.py       # Workload deployment and monitoring
│   │   ├── metrics_collector.py       # Performance data collection
│   │   ├── results_comparator.py      # Statistical comparison analysis
│   │   ├── report_generator.py        # Results reporting & visualization
│   │   ├── troubleshoot.py           # Environment validation & debugging
│   │   ├── visualizer.py             # Visualization generation
│   │   ├── benchmark_config.yaml     # Main configuration file
│   │   └── requirements.txt          # Python dependencies
│   ├── workloads/                    # Workload definition manifests
│   └── results/                      # Generated results and reports
```

## Prerequisites

- Kubernetes cluster (v1.28.11+) within a disctributed edge-cloud environment
- MinIO distributed storage deployment
- Python 3.8+ with required dependencies
- Data-locality-aware scheduler deployed and registered
- Node capability daemon running across all nodes

### Infrastructure Requirements

The framework requires a heterogeneous edge-cloud cluster topology with:
- Minimum 4+ nodes with edge and cloud designations
- Proper node labeling for topology awareness
- Network constraints reflecting realistic edge-cloud bandwidth limitations
- MinIO storage instances distributed across geographic regions

## Configuration

### Benchmark Configuration (`benchmark_config.yaml`)

The primary configuration file controls all experimental parameters:

```yaml
schedulers:
  - name: "default-scheduler"
    description: "Default Kubernetes scheduler"
  - name: "data-locality-scheduler" 
    description: "Data-locality-aware scheduler with MCDM"
    weights:
      resourceWeight: 0.20
      dataLocalityWeight: 0.40
      nodeTypeWeight: 0.15

workloads:
  - name: "etl-processing"
    description: "Data transformation pipeline"
    iterations: 9
    data_intensity: "high"
    max_wait_time: 900
  - name: "image-processing"
    description: "Computer vision workload"
    iterations: 9
    data_intensity: "medium"
    compute_intensity: "high"

metrics:
  collect_node_metrics: true
  collect_pod_metrics: true
  data_transfer_metrics: true
  detailed_data_locality: true
  cross_region_transfer_tracking: true

execution:
  max_wait_time: 1250
  cleanup_after_run: true
  validation_level: "thorough"
```


### Storage Configuration (`storage_config.yaml`)

Defines distributed MinIO deployment across cluster regions:
- `minio-central` on cloud nodes
- `minio-edge-region1` and `minio-edge-region2` on respective edge nodes
- Geographic distribution matching cluster topology

## Installation and Setup

### 1. Deploy Core System Components

```bash
# Deploy the data-locality-aware scheduler and node daemon
kubectl apply -f deployment/scheduler.yaml
kubectl apply -f deployment/node-capability-daemon.yaml
kubectl apply -f deployment/storage.yaml

# Install framework dependencies
pip install -r benchmarks/simulated/framework/requirements.txt

```

### 2. Environment Validation

```bash
# Validate complete system configuration
cd benchmarks/simulated/framework
python troubleshoot.py --all
```

This validation ensures:

- Node labeling verification
- MinIO endpoint accessibility
- Scheduler registration with Kubernetes API 
- Data transfer connectivity tests


### 3. Configuration Customization

Edit `benchmark_config.yaml` to match your cluster topology and evaluation requirements:

- Adjust workload iterations and timeouts
- Modify scheduler MCDM weights
- Configure metrics collection preferences
- Set execution parameters

## Running Benchmarks

### Execution:

```bash
cd benchmarks/simulated/framework
python benchmark_runner.py --config benchmark_config.yaml
```

### Advanced Options

```bash
python benchmark_runner.py \
    --config benchmark_config.yaml \
    --output-dir results/custom-run \
    --run-id $(date +%Y%m%d-%H%M%S)
```


### Execution Protocol

The framework follows a rigorous three-stage methodology:

1. **Environment Preparation** - Namespace setup, storage deployment, data initialization
2. **Workload Execution** - Scheduler alternation across multiple iterations 
3. **Results Analysis** - Metrics collection, statistical comparison, report generation

Each experimental cycle includes:
- Environment reset to ensure consistent starting conditions
- Data distribution according to workload specifications
- Alternating scheduler execution (default vs. data-locality-aware)
- Pod placement monitoring and metrics collection
- Complete state cleanup between iterations

## Output and Results

### Generated Artifacts

The framework produces comprehensive evaluation results:


```bash
results/
├── benchmark_results_[RUN_ID].json    # Raw metrics data
├── benchmark_report_[RUN_ID].md       # Human-readable analysis
├── comparison_[RUN_ID].json           # Statistical comparisons
├── visualizations/
│   ├── data_locality_comparison.pdf   # Locality score improvements
│   ├── network_transfer_analysis.pdf  # Data movement patterns
│   ├── scheduling_latency.pdf         # Performance overhead analysis
│   └── node_placement_heatmap.pdf     # Geographic distribution
└── benchmark_run.log                  # Execution logs
```


### Key Metrics

The framework evaluates schedulers across multiple dimensions:
- **Data Locality Scores** - Overall, weighted, and size-weighted locality metrics
- **Network Transfer Efficiency** - Local vs. cross-region data movement
- **Placement Preferences** - Edge preference satisfaction and node distribution
- **Scheduling Latency** - Decision overhead and placement timing
- **Resource Utilization** - CPU, memory, and storage efficiency patterns


## Customization and Extension

### Workload Scenarios

Add custom workloads by:
1. Creating YAML manifests in `workloads/` directory
2. Adding workload definitions to `benchmark_config.yaml`
3. Following annotation patterns for data dependencies and resource requirements

### Scheduler Parameter Exploration

Modify MCDM weights in the scheduler configuration section to explore alternative optimization strategies. All weight distributions must satisfy the constraint: Σwᵢ = 1.

### Cluster Topology Adaptation

Adjust storage configuration and node topology definitions to match available infrastructure while maintaining minimum requirements for valid data locality evaluation.

## Troubleshooting

### Common Issues and Solutions

```bash
# Reset entire benchmark environment
python troubleshoot.py --reset

# Redeploy storage services  
python troubleshoot.py --deploy-storage

# Individual connectivity checks
python troubleshoot.py --check-connectivity

```


### Typical problems:

Pod placement failures → Verify node labels and resource availability
MinIO connectivity issues → Check storage endpoints and network policies
Scheduler selection errors → Confirm scheduler registration with Kubernetes API
Inconsistent measurements → Ensure cluster resource availability and stable network conditions

### Validation Commands

```bash
# Check scheduler registration
kubectl get pods -n data-locality-scheduler

# Verify node capability daemon status
kubectl get daemonset node-capability-daemon

# Test MinIO connectivity
kubectl exec -it minio-test-pod -- mc ping minio-central/test-bucket
```

## Results Analysis and Visualization

### Generate Results Visualizations

```bash
python visualizer.py \
    --input results/benchmark_results_[RUN_ID].json \
    --output-dir visualizations \
    --format pdf
```