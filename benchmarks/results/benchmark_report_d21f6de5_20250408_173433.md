# Data Locality Scheduler Benchmark Report

Generated: 2025-04-08 17:34:33
Run ID: d21f6de5

## Cluster Information

Total nodes: 6
Edge nodes: 5
Cloud nodes: 1

## Data Locality Analysis

This section provides detailed insights into how data locality awareness affects workload performance:

### Transfer Cost Analysis

| Workload | Scheduler | Total Data (MB) | Network Transfer (MB) | Transfer Reduction |
|----------|-----------|----------------|----------------------|-------------------|
| cross-region-data-processing | data-locality-scheduler | 1245.00 | 588.00 | 28.83% |
| cross-region-data-processing | default-scheduler | 1245.00 | 820.00 | - |
| ml-training-pipeline | data-locality-scheduler | 2445.00 | 1225.00 | 18.00% |
| ml-training-pipeline | default-scheduler | 2445.00 | 1500.00 | - |

## Workload Results

### cross-region-data-processing

#### Data Locality Comparison

| Scheduler | Data Locality Score | Local Data % | Cross-Region % |
|-----------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.4545 | 54.8% | 18.1% |
| default-scheduler | 0.3333 | 34.1% | 18.1% |

**Data Locality Improvement: 39.36%**

**Local Data Access Improvement: 57.76%**

#### Network Data Transfer Comparison

| Scheduler | Total Data (MB) | Local Data (MB) | Cross-Region (MB) | Edge-to-Cloud (MB) | Local % | Cross-Region % |
|-----------|----------------|----------------|------------------|-------------------|---------|---------------|
| data-locality-scheduler | 1245.00 | 645.00 | 225.00 | 300.00 | 51.8% | 18.1% |
| default-scheduler | 1245.00 | 425.00 | 225.00 | 195.00 | 34.1% | 18.1% |

**Local Data Access Improvement: 51.76%**

**Cross-Region Data Transfer Reduction: 0.00%**

#### Scheduling Latency Comparison

| Scheduler | Mean Latency (s) | Min Latency (s) | Max Latency (s) |
|-----------|------------------|-----------------|------------------|
| data-locality-scheduler | 1.2500 | 1.0000 | 3.0000 |
| default-scheduler | 0.0833 | 0.0000 | 0.16670 |

**Scheduling Latency Improvement: -2600.00%**

#### Node Placement Distribution

| Scheduler | Edge Placements | Cloud Placements | Edge % | Cloud % |
|-----------|----------------|-----------------|--------|----------|
| data-locality-scheduler | 6 | 6 | 50.0% | 50.0% |
| default-scheduler | 6 | 6 | 50.0% | 50.0% |

**Edge Resource Utilization Improvement: 0.00%**

### ml-training-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Local Data % | Cross-Region % |
|-----------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.7223 | 47.7% | 0.0% |
| default-scheduler | 0.3846 | 38.7% | 27.0% |

**Data Locality Improvement: 89.00%**

**Local Data Access Improvement: 7.94%**

#### Network Data Transfer Comparison

| Scheduler | Total Data (MB) | Local Data (MB) | Cross-Region (MB) | Edge-to-Cloud (MB) | Local % | Cross-Region % |
|-----------|----------------|----------------|------------------|-------------------|---------|---------------|
| data-locality-scheduler | 2445.00 | 1020.00 | 0.00 | 75.00 | 41.7% | 0.0% |
| default-scheduler | 2445.00 | 945.00 | 660.00 | 105.00 | 38.7% | 27.0% |

**Local Data Access Improvement: 7.94%**

**Cross-Region Data Transfer Reduction: 100.00%**

#### Node Placement Distribution

| Scheduler | Edge Placements | Cloud Placements | Edge % | Cloud % |
|-----------|----------------|-----------------|--------|----------|
| data-locality-scheduler | 6 | 9 | 40.0% | 60.0% |
| default-scheduler | 6 | 9 | 40.0% | 60.0% |

**Edge Resource Utilization Improvement: 0.00%**


### Edge Resource Utilization

### Edge Resource Utilization

| Scheduler | Average Edge Utilization |
|-----------|---------------------------|
| data-locality-scheduler | 45.00% |
| default-scheduler | 45.00% |

### Data Transfer Efficiency

| Scheduler | Local Data % | Cross-Region % | Edge-to-Cloud % |
|-----------|-------------|---------------|----------------|
| data-locality-scheduler | 46.76% | 9.04% | 13.58% |
| default-scheduler | 36.39% | 22.53% | 9.98% |

## Overall Summary

| Metric | Average Improvement |
|--------|---------------------|
| Data Locality | 58.18% |
| Local Data Access | 36.85% |
| Scheduling Latency | -1700.00% |
| Cross-Region Transfer Reduction | 55.00% |

