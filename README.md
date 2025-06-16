# Data-Locality-Aware Kubernetes Scheduler

> **Bachelor's Thesis Project**  
> *Data-Locality-Aware Scheduling for Serverless Containerized Workflows across the Edge-Cloud Continuum*  
> **Author:** Andrei-David Nan  
> **Supervisor:** Lect. Dr. Adrian Spătaru  
> **Institution:** West University of Timișoara, Faculty of Mathematics and Computer Science

## Overview

This project implements a data-locality-aware scheduler extension for Kubernetes that optimizes containerized workload placement across edge-cloud environments. The scheduler uses a Multi-Criteria Decision Making (MCDM) algorithm to balance resource availability, node capabilities, and data locality when making scheduling decisions.

## Architecture

The system consists of three main components:

### 1. Data-Locality-Aware Scheduler (`cmd/scheduler`)
- Extends Kubernetes scheduler with MCDM algorithm
- Maintains Bandwidth Graph and Data Index for locality decisions
- Registers as alternative scheduler (`schedulerName: "data-locality-scheduler"`)
- Implements dynamic priotity function weight adjustment based on workload classification

### 2. Node Capability Daemon (`cmd/node-daemon`)
- Runs as DaemonSet across all cluster nodes
- Detects hardware capabilities and storage characteristics

- Updates node labels with capability information
- Discovers local data objects (MinIO buckets, volumes), and works together with the Storage Index of the scheduler which accounts for data item placements. 

### 3. Knative Integration Webhook (`integration/knative`)
- Intercepts Knative service deployments
- Enriches serverless functions with scheduling annotations
- Enables data-locality optimization for FaaS workloads

## Project Structure

```bash
├── cmd/                    
│   ├── node-daemon/        # Node capability detection daemon
│   └── scheduler/          # Data-locality-aware scheduler
├── pkg/                    
│   ├── daemon/             # Node daemon implementation
│   ├── scheduler/          # Scheduler algorithms and logic
│   └── storage/            # Storage abstraction and indexing
├── deployments/            
│   ├── 00-core/           # Scheduler and daemon deployments
│   ├── 01-storage/        # MinIO storage setup
│   ├── 02-test/           # Test workload definitions
│   └── 03-validation/     # Validation and stress tests
├── benchmarks/            
├── integration/knative/   # Knative serverless integration
├── config/               # Configuration files
└── build/                # Docker build files
```

## Quick Start

### Prerequisites

- Kubernetes cluster (v1.23+)
- `kubectl` configured
- Docker (for building custom images)

### Basic Deployment

1. **Deploy core components:**
   ```bash
   kubectl apply -f deployments/00-core/
    ```   
2. **Set up storage (optional)**

    ```bash
    kubectl apply -f deployments/01-storage/
    ```


### Using the scheduler

Specify the scheduler in your pod spec:

```bash
apiVersion: v1
kind: Pod
metadata:
  name: my-workload
spec:
  schedulerName: "data-locality-scheduler"
  containers:
  - name: app
    image: my-app:latest
```

## Configuration

The scheduler behavior can be customized via `config/scheduler-config.yaml`:

```yaml
scheduler:
  weights:
    default:
      resource: 0.20
      affinity: 0.10
      nodeType: 0.15
      capabilities: 0.15
      dataLocality: 0.40
    dataIntensive:
      dataLocality: 0.70
      resource: 0.10
```

## Knative Integration

For serverless workloads using Knative:

1. **Deploy the webhook**:

```bash
  kubectl apply -f integration/knative/manifests/
```


2. **Deploy Knative services normally** - they will automatically use the data-locality scheduler:

```bash
  kubectl apply -f - <<EOF
    apiVersion: serving.knative.dev/v1
    kind: Service
    metadata:
      name: eo-fmask-processor
      namespace: default
      annotations:
        scheduler.thesis/data-sources: "landsat-l1,landsat-l2"
        scheduler.thesis/workload-type: "compute-intensive"
    spec:
      template:
        metadata:
          annotations:
            autoscaling.knative.dev/target: "5"
        spec:
          containers:
          - name: fmask-processor
            image: my-registry/eo-fmask:latest
            ports:
            - containerPort: 8080
            env:
            - name: MINIO_ENDPOINT
              value: "minio.storage.svc.cluster.local:9000"
            - name: LANDSAT_BUCKET
              value: "landsat-l1"
            resources:
              requests:
                memory: "2Gi"
                cpu: "1000m"
              limits:
                memory: "4Gi"
                cpu: "2000m"
  EOF
```
3. **Trigger the function with a CloudEvent** containing data dependencies:

```bash
  curl -X POST http://eo-fmask-processor.default.example.com \
    -H "Content-Type: application/json" \
    -H "Ce-Specversion: 1.0" \
    -H "Ce-Type: eo.processing.fmask.request" \
    -H "Ce-Source: eo-pipeline/landsat-processor" \
    -H "Ce-Id: fmask-LC08-039037-20241215" \
    -H "Ce-Subject: landsat-cloud-masking" \
    -d '{
      "specversion": "1.0",
      "type": "eo.processing.fmask.request",
      "source": "eo-pipeline/landsat-processor",
      "id": "fmask-LC08-039037-20241215",
      "data": {
        "dataInputs": [
          {
            "urn": "landsat-l1/LC08_L1TP_B1.tif",
            "size": 45097156,
            "processingTime": 30
          },
          {
            "urn": "landsat-l1/LC08_L1TP_B10.tif",
            "size": 22548578
          }
        ],
        "processingOptions": {
          "computeIntensive": true,
          "preferRegion": "region-1"
        }
      }
    }'
```


## Benchmarking

This project includes a benchmarking framework for evaluating scheduler performance across different workload scenarios.


**For detailed benchmarking instructions, see:** [`benchmarks/README.md`](benchmarks/README.md)

### Quick Benchmark Run

```bash
cd benchmarks/simulated/framework
python benchmark_runner.py --config benchmark_config.yaml
```

The benchmarking framework supports:
- Multiple workload types (ETL, image processing, stream processing, etc.)
- Comparative analysis vs. default Kubernetes scheduler
- Data locality and network topology impact analysis
- Performance metrics and visualizations

## Development

### Building from Source

```bash
# Build scheduler
docker build -f build/Dockerfile.scheduler -t data-locality-scheduler .

# Build node daemon  
docker build -f build/Dockerfile.daemon -t node-capability-daemon .
```

### Running Tests

```bash
# Apply validation tests
kubectl apply -f deployments/03-validation/

# Monitor results
kubectl logs -f deployment/scheduler-validator
```