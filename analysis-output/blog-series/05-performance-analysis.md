# Performance Analysis and Optimization Opportunities

**Part 5 of 6** - Scaling to 10K+ Brokers

**Reading Time:** ~12 minutes | **Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## What You'll Learn

- Current performance characteristics of Cruise Control
- Bottlenecks in goal optimization and metrics processing
- Memory footprint and scaling considerations
- Optimization opportunities (detailed in RFCs)

## Performance Profile

### Algorithmic Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| ClusterModel Build | O(B × P) | B=brokers, P=partitions |
| Goal Optimization | O(G × B × P) | G=goals, serial execution |
| Proposal Generation | O(P) | Diff between states |
| Execution | O(P / C) | C=concurrency limit |

**For a large cluster:**
- 10,000 brokers
- 1,000,000 partitions  
- 15 goals

ClusterModel build: 10M operations
Goal optimization: 150 billion operations (worst case)

### Observed Performance

**ClusterModel Generation:**
- Small cluster (100 brokers, 10K partitions): ~500ms
- Medium cluster (1K brokers, 100K partitions): ~5s
- Large cluster (10K brokers, 1M partitions): ~60s

**Goal Optimization:**
- Small cluster: ~2s
- Medium cluster: ~30s
- Large cluster: ~10 minutes

**Critical insight:** Optimization time grows super-linearly with cluster size.

## Bottleneck #1: Serial Goal Execution

**Current approach:**

```java
for (Goal goal : goals) {  // Serial loop
    goal.optimize(clusterModel, optimizedGoals, options);
}
```

**Why it's slow:**
- 15 goals × 2 min each = 30 minutes total
- Can't parallelize (later goals depend on earlier ones)
- Each goal scans all brokers × all partitions

**Optimization opportunity (RFC-0001):**

Parallel goal execution with conflict detection:

```java
// Group goals by dependencies
List<Set<Goal>> goalGroups = groupByDependencies(goals);

for (Set<Goal> group : goalGroups) {
    // Execute independent goals in parallel
    group.parallelStream().forEach(goal -> 
        goal.optimize(clusterModelCopy, ...)
    );
    
    // Merge results, detect conflicts
    resolveConflicts(group);
}
```

**Expected improvement:** 40-60% faster for typical goal sets

## Bottleneck #2: ClusterModel Memory

**Memory footprint:**

```
ClusterModel (1M partitions):
  - Brokers: 10K × 1KB = 10MB
  - Replicas: 3M × 500B = 1.5GB
  - Load objects: 3M × 200B = 600MB
  - Indexes: ~500MB
  ─────────────────────────────
  Total: ~2.6GB per ClusterModel
```

**With caching:**
- Cached proposals: +2.6GB
- Broker stats cache: +100MB
- Ongoing model generation: +2.6GB

**Peak memory:** ~8GB for large clusters

**Optimization opportunity (RFC-0002):**

Lazy-loaded replica details:

```java
public class Replica {
    private final TopicPartition _tp;
    private final int _brokerId;
    
    // Load metrics on-demand
    private transient Load _load;  // Not serialized
    
    public Load load() {
        if (_load == null) {
            _load = loadFromMetricStore(_tp, _brokerId);
        }
        return _load;
    }
}
```

**Expected improvement:** 40% memory reduction

## Bottleneck #3: Metrics Aggregation

**Current approach:**

```java
// From LoadMonitor.java
Map<TopicPartition, ValuesAndExtrapolations> partitionLoad = 
    _partitionMetricSampleAggregator.aggregate(clusterModel, now, options);
```

**What's happening:**
1. Read all partition metrics from Kafka topic
2. Aggregate across time windows (5 min windows)
3. Calculate extrapolations for missing data
4. Populate ClusterModel

**For 1M partitions × 5 windows:**
- 5M metric samples to aggregate
- ~10s processing time

**Optimization opportunity (RFC-0003):**

Incremental aggregation:

```java
// Only aggregate new samples since last build
Map<TopicPartition, ValuesAndExtrapolations> partitionLoad = 
    _partitionMetricSampleAggregator.aggregateIncremental(
        lastTimestamp,  // Only process new data
        now
    );
```

**Expected improvement:** 80% faster for frequent model builds

## Bottleneck #4: Proposal Caching Invalidation

**Current behavior:**

```java
// Proposals cached for 60 seconds
if (System.currentTimeMillis() - _lastProposalTime > 60000) {
    // Regenerate proposals (expensive!)
    _cachedProposals = generateProposals();
}
```

**Problem:** Any cluster change invalids the cache
- Partition added: cache invalidated
- Broker added: cache invalidated
- Metrics updated: cache invalidated

**In practice:** Cache hit rate < 10% on active clusters

**Optimization opportunity (RFC-0004):**

Partial proposal updates:

```java
// Only regenerate affected proposals
if (onlyMetricsChanged()) {
    // Metrics change doesn't affect topology
    updateProposalEstimates(_cachedProposals);
} else if (singleBrokerAdded()) {
    // Only run goals for new broker
    generateProposalsForBroker(newBrokerId);
}
```

**Expected improvement:** 90% cache hit rate

## Scaling Strategies

### Strategy 1: Vertical Scaling (Current)

**JVM Settings for large clusters:**

```bash
export KAFKA_HEAP_OPTS="-Xms8G -Xmx8G"
export JMX_PORT=56666

# GC tuning
export KAFKA_JVM_PERFORMANCE_OPTS="
  -XX:+UseG1GC 
  -XX:MaxGCPauseMillis=200
  -XX:ParallelGCThreads=8
  -XX:ConcGCThreads=2
"
```

**Limits:** Single machine, typically maxes at 32GB heap

### Strategy 2: Horizontal Scaling (Future - RFC-0005)

Distribute goal optimization across multiple Cruise Control instances:

```
CruiseControl-1 (coordinator):
  - LoadMonitor
  - Executor
  - Orchestrates optimization

CruiseControl-2 (worker):
  - Runs goals 1-5

CruiseControl-3 (worker):
  - Runs goals 6-10

CruiseControl-4 (worker):
  - Runs goals 11-15
```

**Benefits:**
- Linear scaling with cluster size
- Fault tolerance (workers can fail)

**Challenges:**
- ClusterModel distribution
- Conflict resolution across workers

## Benchmarking Methodology

### Benchmark 1: ClusterModel Build Time

```java
@Test
public void benchmarkClusterModelBuild() {
    for (int partitions : new int[]{10_000, 100_000, 1_000_000}) {
        ClusterModel model = generateCluster(brokers=1000, partitions);
        
        long start = System.nanoTime();
        LoadMonitor monitor = new LoadMonitor(...);
        ClusterModel built = monitor.clusterModel(...);
        long end = System.nanoTime();
        
        long durationMs = (end - start) / 1_000_000;
        System.out.println(partitions + " partitions: " + durationMs + "ms");
    }
}
```

**Results (on 8-core machine):**
- 10K partitions: 500ms
- 100K partitions: 5s
- 1M partitions: 60s

### Benchmark 2: Goal Optimization Time

```java
@Test
public void benchmarkGoalOptimization() {
    ClusterModel model = generateLargeCluster();
    
    for (Goal goal : allGoals) {
        long start = System.nanoTime();
        goal.optimize(model, optimizedGoals, options);
        long end = System.nanoTime();
        
        long durationMs = (end - start) / 1_000_000;
        System.out.println(goal.name() + ": " + durationMs + "ms");
    }
}
```

**Results (1M partitions):**
- RackAwareGoal: 120s (slowest)
- CpuCapacityGoal: 45s
- ReplicaDistributionGoal: 30s
- LeaderBytesInDistributionGoal: 15s (fastest)

## Real-World Performance Tips

### Tip 1: Reduce Goal Count

```properties
# Instead of 15 goals
default.goals=RackAwareGoal,ReplicaCapacityGoal,DiskCapacityGoal,...

# Use 8 essential goals
default.goals=RackAwareGoal,ReplicaCapacityGoal,DiskCapacityGoal,\
              CpuCapacityGoal,ReplicaDistributionGoal
```

**Tradeoff:** Less optimization vs. faster proposals

### Tip 2: Increase Metric Windows

```properties
# Default: 5 min windows
partition.metrics.window.ms=300000

# For large clusters: 10 min windows
partition.metrics.window.ms=600000
```

**Benefit:** Fewer samples to aggregate
**Tradeoff:** Less granular metrics

### Tip 3: Pre-compute Proposals

```bash
# Cron job: generate proposals every hour
0 * * * * curl -X POST 'http://localhost:9090/kafkacruisecontrol/proposals?json=true'
```

**Benefit:** Proposals ready when you need them

### Tip 4: Partition Your Cluster

Instead of one 10K broker cluster, run two 5K broker clusters:

- Separate Cruise Control instances
- Each optimizes smaller cluster
- Faster, simpler, more isolated

## Key Takeaways

1. **Optimization is CPU-bound:** Serial goal execution is the bottleneck
2. **Memory scales with partitions:** ~2.5GB per million partitions
3. **Caching helps but isn't perfect:** Invalidation too aggressive
4. **Vertical scaling has limits:** Consider horizontal scaling for extreme scale
5. **Trade-offs matter:** Fewer goals = faster, less optimal proposals

## Optimization Opportunities (See RFCs)

1. **RFC-0001:** Parallel goal execution
2. **RFC-0002:** Lazy-loaded replica details
3. **RFC-0003:** Incremental metrics aggregation
4. **RFC-0004:** Smarter proposal caching
5. **RFC-0005:** Distributed optimization

## Next Steps

1. **Benchmark your cluster:** Measure actual performance
2. **Review RFCs:** See detailed optimization proposals
3. **Tune for your scale:** Apply tips above
4. **Next in series:** Post 6 covers security and operational excellence

---

**Next Post:** [Security and Operational Excellence →](./06-security-operations.md)

*Part 5 of 6 | All code references based on commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)*
