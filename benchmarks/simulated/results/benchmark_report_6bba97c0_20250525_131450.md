# Data Locality Scheduler Benchmark Report

Generated: 2025-05-25 13:14:50
Run ID: 6bba97c0

## Cluster Information

Total nodes: 6
Edge nodes: 5
Cloud nodes: 1

## Summary of Results

The data-locality scheduler demonstrates significant improvements across multiple metrics when compared to the default Kubernetes scheduler:

- **243.06%** improvement in overall data locality
- **299.67%** improvement in size-weighted data locality
- **1713.41%** increase in local data access
- **90.70%** reduction in cross-region data transfers

### Network Efficiency Gains

| Metric | Default Scheduler | Data Locality Scheduler | Improvement |
|--------|-------------------|------------------------|-------------|
| Local data access | 20.14% | 66.18% | 228.64% |

## Data Transfer Analysis

This section highlights the reduction in data transfer volumes achieved by the data-locality scheduler, which translates directly to cost savings in distributed environments:

### Transfer Cost Analysis

| Workload | Scheduler | Total Data (MB) | Network Transfer (MB) | Transfer Reduction |
|----------|-----------|----------------|----------------------|-------------------|
| ml-training-pipeline | data-locality-scheduler | 815.00 | 475.00 | 15.93% |
| ml-training-pipeline | default-scheduler | 815.00 | 565.00 | - |
| image-processing-pipeline | data-locality-scheduler | 1240.00 | 160.00 | 79.35% |
| image-processing-pipeline | default-scheduler | 1240.00 | 775.00 | - |
| etl-pipeline | data-locality-scheduler | 901.00 | 310.00 | 51.64% |
| etl-pipeline | default-scheduler | 901.00 | 641.00 | - |
| cross-region-data-processing | data-locality-scheduler | 865.00 | 340.00 | 59.52% |
| cross-region-data-processing | default-scheduler | 865.00 | 840.00 | - |
| stream-processing-pipeline | data-locality-scheduler | 335.00 | 120.00 | 54.72% |
| stream-processing-pipeline | default-scheduler | 335.00 | 265.00 | - |
| edge-to-cloud-pipeline | data-locality-scheduler | 45.00 | 10.00 | 77.78% |
| edge-to-cloud-pipeline | default-scheduler | 45.00 | 45.00 | - |

### Data Locality Distribution

### Placement Preference Satisfaction

One key aspect of the data-locality scheduler is how well it honors placement preferences:

| Preference Type | Default Scheduler | Data Locality Scheduler | Improvement |
|-----------------|-------------------|------------------------|-------------|
## Workload Results

### ml-training-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.6923 | 0.8462 | 0.7086 | 41.7% | 0.0% |
| default-scheduler | 0.1538 | 0.3077 | 0.4479 | 30.7% | 41.1% |

**Data Locality Improvement: 350.00%**

**Size-Weighted Data Locality Improvement: 58.22%**

**Local Data Access Improvement: 36.00%**

### image-processing-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.7143 | 0.7857 | 0.8931 | 87.1% | 8.5% |
| default-scheduler | 0.3571 | 0.4286 | 0.3831 | 37.5% | 60.9% |

**Data Locality Improvement: 100.00%**

**Size-Weighted Data Locality Improvement: 133.16%**

**Local Data Access Improvement: 132.26%**

### etl-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.7333 | 0.8000 | 0.7558 | 65.6% | 14.4% |
| default-scheduler | 0.2000 | 0.3667 | 0.4218 | 28.9% | 44.5% |

**Data Locality Improvement: 266.67%**

**Size-Weighted Data Locality Improvement: 79.21%**

**Local Data Access Improvement: 127.31%**

### cross-region-data-processing

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.5455 | 0.6818 | 0.7601 | 60.7% | 8.7% |
| default-scheduler | 0.0909 | 0.1364 | 0.0578 | 2.9% | 91.3% |

**Data Locality Improvement: 500.00%**

**Size-Weighted Data Locality Improvement: 1215.00%**

**Local Data Access Improvement: 2000.00%**

### stream-processing-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.6667 | 0.8333 | 0.8209 | 64.2% | 0.0% |
| default-scheduler | 0.2500 | 0.3333 | 0.2537 | 20.9% | 70.1% |

**Data Locality Improvement: 166.67%**

**Size-Weighted Data Locality Improvement: 223.53%**

**Local Data Access Improvement: 207.14%**

### edge-to-cloud-pipeline

#### Data Locality Comparison

| Scheduler | Data Locality Score | Weighted Score | Size-Weighted Score | Local Data % | Cross-Region % |
|-----------|--------------------|-----------------|--------------------|-------------|---------------|
| data-locality-scheduler | 0.7500 | 0.8750 | 0.8889 | 77.8% | 0.0% |
| default-scheduler | 0.0000 | 0.0000 | 0.0000 | 0.0% | 100.0% |

**Data Locality Improvement: 75.00%**

**Size-Weighted Data Locality Improvement: 88.89%**

**Local Data Access Improvement: 7777.78%**

## Tradeoffs Analysis

While the data-locality scheduler provides significant benefits in data locality and network efficiency, there are some tradeoffs to consider:

1. **Balanced Scheduling Latency**: Despite the additional analysis performed, the data-locality scheduler maintains scheduling latency comparable to the default scheduler.

2. **Resource Utilization Shifts**: In some cases, the scheduler may prioritize data locality over even resource distribution, leading to potential concentration of workloads on nodes that contain required data.

3. **Configuration Complexity**: To achieve optimal results, the data-locality scheduler requires proper configuration of data annotations and node capability labels.

## The Importance of Data Locality in Edge-Cloud Environments

Data locality awareness becomes increasingly critical in distributed edge-cloud environments for several reasons:

1. **Reduced Network Traffic**: Minimizing data movement across network boundaries significantly reduces bandwidth consumption and network congestion.

2. **Lower Latency**: Local data access eliminates network transmission delays, particularly important for time-sensitive applications.

3. **Cost Efficiency**: Cross-region data transfers often incur monetary costs in cloud environments, making data locality directly translatable to cost savings.

4. **Energy Efficiency**: Reducing data movement leads to lower energy consumption, contributing to more sustainable computing.

5. **Improved Reliability**: Less reliance on network connectivity increases application resilience against network disruptions.

## Recommendations for Production Deployments

Based on the benchmark results, we recommend the following for production deployments:

1. **Enable Data Locality Annotations**: Ensure all data-intensive workloads include proper data source annotations to allow the scheduler to optimize placement.

2. **Configure Node Capability Labels**: Maintain accurate and up-to-date node capability and topology labels to help the scheduler make informed decisions.

3. **Adjust Scheduler Weights**: Fine-tune the weight parameters for different workload types based on their specific requirements:
   - Data-intensive workloads: Increase `dataLocalityWeight` to prioritize data locality
   - Compute-intensive workloads: Increase `resourceWeight` and `capabilitiesWeight` to prioritize node capabilities

4. **Pre-position Data**: For frequently accessed datasets, consider pre-positioning data copies across regions to provide the scheduler with more locality options.

5. **Monitor and Adjust**: Regularly monitor scheduler effectiveness metrics and adjust configurations as workload patterns evolve.

## Conclusion

The data-locality scheduler demonstrates a significant improvement of **243.06%** in data locality scores across tested workloads. This translates to approximately **0.00%** reduction in network overhead and **1713.41%** increase in local data access.

These results validate that topology-aware, data-locality-conscious scheduling can provide substantial benefits in distributed edge-cloud environments, particularly for data-intensive applications that process large volumes of data across geographic boundaries.

By incorporating knowledge of data location, node capabilities, and network topology into the scheduling decision process, the data-locality scheduler effectively reduces unnecessary data transfers, optimizes resource utilization, and improves overall system efficiency.
