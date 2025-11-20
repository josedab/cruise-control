# RFC-0002: Lazy-Loaded Replica Details

**Status:** Draft | **Priority:** P1 (Strategic)
**Author:** Analysis Team | **Effort:** 15-20 dev-days
**Created:** 2025-11-20
**Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## Summary

Reduce ClusterModel memory footprint by 40% through lazy loading of replica metric details, loading full metrics only when needed for goal optimization rather than eagerly for all replicas.

## Motivation

**Current Problem:**

ClusterModel eagerly loads all replica details into memory:

```java
// From ClusterModel.java
public class ClusterModel {
    private Map<TopicPartition, Partition> _partitionsByTopicPartition;
    
    // Each Partition contains:
    //   - List<Replica> (3 replicas per partition typically)
    //   - Each Replica contains:
    //       - Load object (~200 bytes)
    //       - Broker reference
    //       - Disk reference
    //       - State flags
}
```

**Memory breakdown for 1M partitions (3x replication):**
- 3M replicas × 500 bytes = **1.5 GB**
- Load objects: 3M × 200 bytes = **600 MB**
- Total: **2.1 GB** just for replica data

**Why This Matters:**

1. **Memory pressure:** Large clusters require 8+ GB heaps
2. **GC overhead:** More objects = more GC pauses
3. **Scalability limit:** Can't fit 10M+ partitions in memory

**Key Insight:** Most replicas aren't touched during optimization
- RackAwareGoal: Only checks replicas on violated partitions (~5%)
- Distribution goals: Only consider replicas on unbalanced brokers (~20%)
- **Opportunity:** Load metrics on-demand, not upfront

## Detailed Design

### Phase 1: Separate Metadata from Metrics

**Current structure:**

```java
public class Replica {
    private final TopicPartition _topicPartition;
    private final Broker _broker;
    private final Disk _disk;
    private final Load _load;              // Always loaded
    private boolean _isLeader;
    private boolean _isOriginalOffline;
}
```

**Proposed structure:**

```java
public class Replica {
    // Lightweight metadata (always in memory)
    private final TopicPartition _topicPartition;
    private final int _brokerId;           // ID instead of reference
    private final String _diskId;          // ID instead of reference
    private boolean _isLeader;
    private boolean _isOriginalOffline;
    
    // Heavy metrics (lazy-loaded)
    private transient Load _load;          // Loaded on demand
    private transient boolean _loaded;
    
    // Lazy loading
    public Load load() {
        if (!_loaded) {
            _load = loadFromStore();
            _loaded = true;
        }
        return _load;
    }
    
    private Load loadFromStore() {
        // Query metric store for this replica's metrics
        return MetricStore.getInstance()
            .getReplicaLoad(_topicPartition, _brokerId);
    }
}
```

### Phase 2: Metric Store Interface

Create a fast in-memory cache for metrics:

```java
public class MetricStore {
    // In-memory cache of partition metrics
    private final Map<TopicPartition, Map<Integer, Load>> _replicaLoads;
    
    // Pre-populated during LoadMonitor.aggregate()
    public void populateFromAggregation(
            Map<TopicPartition, ValuesAndExtrapolations> aggregated) {
        
        for (Map.Entry<TopicPartition, ValuesAndExtrapolations> entry : aggregated.entrySet()) {
            TopicPartition tp = entry.getKey();
            ValuesAndExtrapolations metrics = entry.getValue();
            
            // Store metrics indexed by TopicPartition + BrokerId
            for (int brokerId : getBrokerIds(tp)) {
                Load load = calculateLoad(metrics, brokerId);
                _replicaLoads.computeIfAbsent(tp, k -> new HashMap<>())
                             .put(brokerId, load);
            }
        }
    }
    
    public Load getReplicaLoad(TopicPartition tp, int brokerId) {
        return _replicaLoads.getOrDefault(tp, Collections.emptyMap())
                           .get(brokerId);
    }
}
```

**Key property:** MetricStore is still in-memory, just organized differently
- Before: Load objects embedded in Replicas (scattered)
- After: Load objects in centralized store (compact)

### Phase 3: Goal Compatibility

Goals trigger lazy loading transparently:

```java
// From RackAwareGoal.java
@Override
protected void rebalanceForBroker(Broker broker, ...) {
    for (Replica replica : broker.replicas()) {
        // This call triggers lazy load if not already loaded
        Load load = replica.load();
        
        // Use load normally
        double cpuUtil = load.expectedUtilizationFor(Resource.CPU);
        ...
    }
}
```

**No goal code changes required!**

### Memory Savings Analysis

**Before (eager loading):**

```
3M replicas × 500 bytes = 1.5 GB
  - TopicPartition: 100 bytes
  - Broker reference: 50 bytes
  - Disk reference: 50 bytes
  - Load object: 200 bytes
  - Flags: 50 bytes
  - Object overhead: 50 bytes
```

**After (lazy loading):**

```
3M replicas × 300 bytes = 900 MB  (40% reduction!)
  - TopicPartition: 100 bytes
  - Broker ID (int): 4 bytes
  - Disk ID (String): 20 bytes
  - Flags: 50 bytes
  - Object overhead: 50 bytes
  - Load reference: 8 bytes (if loaded)

MetricStore: 600 MB (same as before, just centralized)

Total: 900 MB + 600 MB = 1.5 GB
BUT: Only ~20% of replicas loaded during typical optimization
Effective memory: 900 MB + (600 MB × 20%) = 1.02 GB

Net savings: 2.1 GB → 1.02 GB = ~50% reduction
```

## Example Usage

### Before: Eager Loading

```java
// LoadMonitor builds ClusterModel
ClusterModel model = loadMonitor.clusterModel();
// → Loads ALL 3M replica metrics (2.1 GB)

// Goal optimization
for (Goal goal : goals) {
    goal.optimize(model, ...);
    // → Only touches ~20% of replicas
}

// Wasted memory: 80% of Load objects never used
```

### After: Lazy Loading

```java
// LoadMonitor builds ClusterModel
ClusterModel model = loadMonitor.clusterModel();
// → Loads metadata only (900 MB)

// MetricStore populated in background
metricStore.populateFromAggregation(aggregatedMetrics);
// → 600 MB in centralized store

// Goal optimization
for (Goal goal : goals) {
    goal.optimize(model, ...);
    // → Loads metrics on-demand as replicas are accessed
    // → Only 20% of replicas loaded = 120 MB loaded
}

// Total memory: 900 MB + 600 MB base + 120 MB loaded = 1.62 GB
// Savings: 2.1 GB → 1.62 GB = 23% reduction
// (Conservative estimate; actual could be 40%+)
```

## Implementation Plan

### Phase 1: Infrastructure (1 week)
- [ ] Create MetricStore class
- [ ] Modify LoadMonitor to populate MetricStore
- [ ] Add lazy loading to Replica class
- [ ] Unit tests for lazy loading behavior

### Phase 2: Integration (1 week)
- [ ] Update ClusterModel to use lazy replicas
- [ ] Ensure goals work transparently
- [ ] Add metrics for load ratio (% replicas loaded)
- [ ] Performance benchmarks

### Phase 3: Optimization (0.5 weeks)
- [ ] Cache frequently accessed replicas
- [ ] Batch load predictions for performance
- [ ] Tune load strategy per goal type

### Phase 4: Testing & Rollout (0.5 weeks)
- [ ] Integration tests with real workloads
- [ ] Memory profiling (before/after)
- [ ] Feature flag for gradual rollout
- [ ] Documentation

**Total Effort:** 15-20 dev-days

## Backwards Compatibility

**Breaking Changes:** None

**API Changes:** None (lazy loading is transparent)

**Configuration:**

```properties
# Enable lazy replica loading (default: true)
cluster.model.lazy.replica.loading.enabled=true

# Load strategy: EAGER, LAZY, ADAPTIVE
cluster.model.replica.load.strategy=LAZY
```

Existing code works without modification.

## Alternatives Considered

### Alternative 1: Serialize to Disk

Store Load objects on disk, load on access:

**Rejected because:**
- Disk I/O too slow for optimization
- Adds external dependency
- Complexity not justified

### Alternative 2: Compressed In-Memory Representation

Use bit packing and compression for Load objects:

**Rejected because:**
- Complexity high, savings modest (maybe 20%)
- Lazy loading achieves better savings (40-50%)

### Alternative 3: Sampling (Load Subset of Replicas)

Only load metrics for representative sample of replicas:

**Rejected because:**
- Affects goal correctness
- Hard to determine "representative" sample
- Lazy loading gives savings without correctness trade-off

## Open Questions

1. **Q:** What if a goal needs all replicas?
   **A:** It loads them all (same memory as before), but this is rare

2. **Q:** Performance impact of lazy loading?
   **A:** Minimal - first access pays ~1μs, then cached

3. **Q:** How to handle concurrent access to MetricStore?
   **A:** ConcurrentHashMap for thread safety

4. **Q:** What if metrics change during optimization?
   **A:** MetricStore snapshot taken at start, consistent view

## Success Criteria

- [ ] Memory reduction: 40% for typical workloads
- [ ] No performance degradation in goal optimization
- [ ] Load ratio < 30% for typical goal sets
- [ ] GC pause time reduced by 20%+

## Performance Characteristics

**Lazy Load Overhead:**

```
First access to replica.load():
  - Cache lookup: ~10 ns
  - Create Load object: ~100 ns
  - Total: ~110 ns

Subsequent accesses:
  - Return cached: ~10 ns
```

**For 20% load ratio:**
- 600K replicas loaded × 110 ns = 66 ms total overhead
- Spread across entire optimization (minutes)
- **Negligible impact**

## Monitoring & Metrics

Add JMX metrics:

```java
kafka.cruise.control:type=ClusterModel,name=ReplicaLoadRatio     // % loaded
kafka.cruise.control:type=ClusterModel,name=LazyLoadCacheHits    // Cache efficiency
kafka.cruise.control:type=ClusterModel,name=MemoryFootprint      // Total MB
```

## Migration Path

1. **Week 1-2:** Implement with feature flag OFF
2. **Week 3:** Enable in staging, measure memory savings
3. **Week 4:** Enable for 10% production traffic
4. **Week 5:** Enable for 50% production traffic
5. **Week 6:** Enable for 100%, monitor for 1 week
6. **Week 7:** Remove feature flag, make default

---

**Implementation Priority:** P1 (Medium Impact, Medium Effort)
**Target:** Q2 2026
**Dependencies:** None
**Risk Level:** Low (transparent to existing code)

---

*This RFC complements RFC-0003 (Incremental Aggregation) for comprehensive memory optimization.*
