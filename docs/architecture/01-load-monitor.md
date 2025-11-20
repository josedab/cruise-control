# LoadMonitor: Metrics Collection and ClusterModel Builder

## Overview

The LoadMonitor is responsible for collecting metrics from Kafka brokers, aggregating them into time windows, and building/maintaining an accurate in-memory representation of the cluster state (ClusterModel). It runs continuously in the background, updating the ClusterModel every 120 seconds (configurable).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        LoadMonitor                           │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐ │
│  │ MetricFetcher    │  │ SampleStore      │  │ ClusterModel│ │
│  │ Manager          │──▶│                  │──▶│ Builder    │ │
│  └──────────────────┘  └──────────────────┘  └────────────┘ │
│         │                      │                     │       │
└─────────┼──────────────────────┼─────────────────────┼───────┘
          │                      │                     │
          ▼                      ▼                     ▼
   ┌─────────────┐      ┌──────────────┐      ┌─────────────┐
   │ __CruiseControl     │ Partition    │      │ ClusterModel│
   │ Metrics Topic│      │ Sample Store │      │ (In-Memory) │
   └─────────────┘      └──────────────┘      └─────────────┘
```

## Components

### 1. MetricFetcherManager

**Responsibilities:**
- Consume metrics from `__CruiseControlMetrics` topic
- Parse raw metric samples
- Forward to MetricSampleAggregator

**Metric Sources:**
- **Broker Metrics:** CPU, network I/O, disk I/O, produced/consumed bytes
- **Topic Metrics:** Bytes in/out per partition
- **Consumer Group Metrics:** Lag, offset

**Sampling Interval:** 60 seconds (configured on broker via `CruiseControlMetricsReporter`)

### 2. MetricSampleAggregator

**Responsibilities:**
- Aggregate raw metrics into time windows
- Compute statistics (min, max, avg, percentiles)
- Store aggregated samples in SampleStore

**Time Windows:**
- **Short:** Hourly windows for past 24 hours
- **Medium:** Daily windows for past 7 days
- **Long:** Weekly windows for past 4 weeks

**Aggregation Functions:**
- **SUM:** Total bytes in/out
- **AVG:** Average CPU, network utilization
- **MAX:** Peak utilization
- **MIN:** Minimum utilization

### 3. SampleStore

**Responsibilities:**
- Persist metric samples (in-memory or external storage)
- Provide efficient access to historical data
- Support time-range queries

**Storage Options:**
- **In-Memory:** Fast, limited by RAM (default)
- **Kafka Topics:** Durable, supports larger history
- **External DB:** Custom implementation (e.g., InfluxDB)

**Configuration:**
```properties
# Use Kafka topics for sample storage
metric.sampler.class=com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler
partition.metric.sample.store.topic=__KafkaCruiseControlPartitionMetricSamples
broker.metric.sample.store.topic=__KafkaCruiseControlModelTrainingSamples

# Retention: 7 days
partition.metrics.window.ms=3600000  # 1 hour
num.partition.metrics.windows=168    # 168 hours = 7 days
```

### 4. ClusterModelBuilder

**Responsibilities:**
- Build ClusterModel from aggregated metrics
- Update model with latest cluster metadata
- Handle broker/partition additions/removals

**Build Process:**

1. **Fetch Cluster Metadata**
   ```java
   // Get brokers, topics, partitions from Kafka
   Cluster cluster = adminClient.describeCluster();
   Map<String, TopicDescription> topics = adminClient.describeTopics();
   ```

2. **Create Broker and Replica Objects**
   ```java
   for (Broker broker : cluster.brokers()) {
     ClusterModel.Broker modelBroker = new ClusterModel.Broker(broker.id());
     for (Partition partition : broker.partitions()) {
       Replica replica = new Replica(partition);
       modelBroker.addReplica(replica);
     }
   }
   ```

3. **Load Metrics from SampleStore**
   ```java
   for (Replica replica : clusterModel.replicas()) {
     List<MetricSample> samples = sampleStore.getSamples(
       replica.topicPartition(),
       timeWindow
     );
     replica.setLoad(aggregateLoad(samples));
   }
   ```

4. **Compute Derived Metrics**
   ```java
   // Compute broker-level aggregations
   for (Broker broker : clusterModel.brokers()) {
     broker.setLoad(sumReplicaLoads(broker.replicas()));
   }
   ```

## ClusterModel Structure

See [ClusterModel Deep Dive](05-cluster-model.md) for detailed structure.

**Key Entities:**
- **Broker:** Represents a Kafka broker with its capacity and current load
- **Replica:** Represents a partition replica with its metrics
- **Rack:** Logical grouping of brokers for rack awareness

## Performance Characteristics

### Build Time

**Formula:** `O(brokers × partitions)`

**Typical Performance:**
- 100 brokers, 10K partitions: ~5 seconds
- 1000 brokers, 100K partitions: ~30 seconds
- 10000 brokers, 1M partitions: ~5 minutes

### Memory Usage

**Formula:** `250KB per replica × replication factor × num partitions`

**Examples:**
- 10K partitions, RF=3: ~7.5 GB
- 100K partitions, RF=3: ~75 GB
- 1M partitions, RF=3: ~750 GB

**Optimization:** Use time window pruning to reduce memory

```properties
# Only keep 24 hours of metrics (vs. 7 days)
num.partition.metrics.windows=24
partition.metrics.window.ms=3600000
```

## Monitoring and Observability

### JMX Metrics

```
kafka.cruisecontrol:type=LoadMonitor,name=ValidPartitionRatio
- Percentage of partitions with valid metrics (should be ~100%)

kafka.cruisecontrol:type=LoadMonitor,name=MonitoredPartitions
- Number of partitions being monitored

kafka.cruisecontrol:type=LoadMonitor,name=NumValidSnapshotWindows
- Number of valid metric snapshot windows

kafka.cruisecontrol:type=LoadMonitor,name=TotalMonitoringTime
- Time spent building ClusterModel
```

### Health Checks

**Valid Partition Ratio:**
- **Healthy:** > 95%
- **Warning:** 90-95%
- **Critical:** < 90%

**If ratio is low:**
1. Check `__CruiseControlMetrics` topic has data
2. Verify `CruiseControlMetricsReporter` configured on brokers
3. Check LoadMonitor logs for errors

## Configuration Reference

| Configuration | Default | Description |
|--------------|---------|-------------|
| `partition.metrics.window.ms` | 3600000 | Metric window size (1 hour) |
| `num.partition.metrics.windows` | 168 | Number of windows to retain (7 days) |
| `min.samples.per.partition.metrics.window` | 1 | Min samples required for valid window |
| `metadata.max.age.ms` | 300000 | How often to refresh cluster metadata |
| `metric.sampler.class` | `CruiseControlMetricsReporterSampler` | Metric source |

## Troubleshooting

### ClusterModel build is slow

**Symptom:** LoadMonitor taking > 5 minutes to build model

**Causes:**
1. Too many partitions (> 1M)
2. Too many time windows (> 168)
3. Inefficient metric sampling

**Solutions:**
```properties
# Reduce time windows
num.partition.metrics.windows=24  # 24 hours instead of 7 days

# Use faster metric sampler
metric.sampler.class=com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler
```

### Low ValidPartitionRatio

**Symptom:** ValidPartitionRatio < 90%

**Causes:**
1. Missing `CruiseControlMetricsReporter` on brokers
2. `__CruiseControlMetrics` topic has low retention
3. Brokers not producing metrics

**Solutions:**
1. Verify reporter configured on all brokers:
   ```properties
   metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter
   ```

2. Check topic retention:
   ```bash
   kafka-topics --describe --topic __CruiseControlMetrics
   # retention.ms should be >= 7 days
   ```

## Related Documentation

- [Architecture Overview](00-overview.md)
- [ClusterModel Structure](05-cluster-model.md)
- [Operations: Monitoring](../operations/monitoring.md)
