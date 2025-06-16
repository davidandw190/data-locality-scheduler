# Data-Locality-Aware Kubernetes Scheduler

> **Bachelor's Thesis Project**  
> *Data-Locality-Aware Scheduling for Serverless Containerized Workflows across the Edge-Cloud Continuum*  
> **Author:** Andrei-David Nan  
> **Supervisor:** Lect. Dr. Adrian Spătaru  
> **Institution:** West University of Timișoara, Faculty of Mathematics and Computer Science

## Overview

This project implements a data-locality-aware scheduler extension for Kubernetes that optimizes containerized workload placement across edge-cloud environments. The scheduler uses a Multi-Criteria Decision Making (MCDM) algorithm to balance resource availability, node capabilities, and data locality when making scheduling decisions.

## Key Features

- **Data-locality optimization** - Minimizes data transfer costs by placing workloads near their required data
- **Edge-cloud aware** - Optimized for distributed environments spanning edge and cloud infrastructure  
- **Serverless integration** - Native support for Knative serverless workloads
- **Non-intrusive deployment** - Runs alongside default Kubernetes scheduler
- **Comprehensive benchmarking** - Built-in evaluation framework with multiple workload scenarios

## Architecture

The system consists of three main components:

### 1. Data-Locality-Aware Scheduler (`cmd/scheduler`)
- Extends Kubernetes scheduler with MCDM algorithm
- Maintains Bandwidth Graph and Data Index for locality decisions
- Registers as alternative scheduler (`schedulerName: "data-locality-scheduler"`)
- Implements dynamic weight adjustment based on workload classification

### 2. Node Capability Daemon (`cmd/node-daemon`)
- Runs as DaemonSet across all cluster nodes
- Detects hardware capabilities and storage characteristics
- Discovers local data objects (MinIO buckets, volumes)
- Updates node labels with capability information

### 3. Knative Integration Webhook (`integration/knative`)
- Intercepts Knative service deployments
- Enriches serverless functions with scheduling annotations
- Enables data-locality optimization for FaaS workloads

## Project Structure

```bash
├── cmd/                    # Main applications
│   ├── node-daemon/        # Node capability detection daemon
│   └── scheduler/          # Data-locality-aware scheduler
├── pkg/                    # Core libraries
│   ├── daemon/             # Node daemon implementation
│   ├── scheduler/          # Scheduler algorithms and logic
│   └── storage/            # Storage abstraction and indexing
├── deployments/            # Kubernetes manifests
│   ├── 00-core/           # Core scheduler and daemon deployments
│   ├── 01-storage/        # MinIO storage setup
│   ├── 02-test/           # Test workload definitions
│   └── 03-validation/     # Validation and stress tests
├── benchmarks/            # Comprehensive benchmarking framework
├── integration/knative/   # Knative serverless integration
├── config/               # Configuration files
├── demo/                 # Demo scenarios and examples
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


Configuration