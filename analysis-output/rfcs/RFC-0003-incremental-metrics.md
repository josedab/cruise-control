# RFC-0003: Incremental Metrics Aggregation

**Status:** Draft
**Author:** Analysis Team
**Created:** 2025-11-20
**Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## Summary

Reduce ClusterModel build time by 80% through incremental metrics aggregation, only processing new samples since the last build instead of reprocessing all historical data.

## Motivation

**Current Problem:**

Every ClusterModel build processes all metric samples:

```java
// From LoadMonitor.java:504-506
Map<TopicPartition, ValuesAndExtrapolations> partitionLoad = 
    _partitionMetricSampleAggregator.aggregate(
        clusterModel,  // Ignores existing data
        now,
        options
    );
```

For a large cluster:
- 1M partitions × 5 windows = 5M samples
- Aggregation time: ~10 seconds
- Build frequency: every 2 minutes
- Wasted CPU: 8/120 = 6.7% continuous

**Why This Matters:**

1. **API Responsiveness:** `/proposals` endpoint slow
2. **Resource Usage:** Constant CPU churn
3. **Scalability:** Limits cluster size

## Detailed Design

### Key Insight

Metric samples are immutable and time-ordered:

```
Window 1: [samples from 00:00-00:05]  ← Never changes
Window 2: [samples from 00:05-00:10]  ← Never changes
Window 3: [samples from 00:10-00:15]  ← Never changes
Window 4: [samples from 00:15-00:20]  ← Currently filling
Window 5: [samples from 00:20-00:25]  ← Empty
```

We only need to process Window 4!

### Implementation

#### Step 1: Track Last Aggregation

```java
public class PartitionMetricSampleAggregator {
    
    // Track when we last aggregated
    private long _lastAggregationTime = 0;
    
    // Cache aggregated results per window
    private Map<Integer, Map<TopicPartition, AggregatedMetrics>> _cachedWindows 
        = new ConcurrentHashMap<>();
}
```

#### Step 2: Incremental Aggregation

```java
public Map<TopicPartition, ValuesAndExtrapolations> aggregate(
        ClusterModel clusterModel,
        long now,
        MetricSamplerOptions options) {
    
    long windowMs = options.windowMs();  // 300000 = 5 minutes
    
    // Determine windows we need
    int currentWindow = (int) (now / windowMs);
    int startWindow = currentWindow - options.numWindows();
    
    Map<TopicPartition, ValuesAndExtrapolations> result = new HashMap<>();
    
    // Process each window
    for (int window = startWindow; window <= currentWindow; window++) {
        if (_cachedWindows.containsKey(window) 
            && window < currentWindow) {  // Past windows never change
            // Use cached data
            mergeCached(result, _cachedWindows.get(window));
        } else {
            // Aggregate this window (only current window in steady state)
            Map<TopicPartition, AggregatedMetrics> windowData = 
                aggregateWindow(window, windowMs, options);
            
            _cachedWindows.put(window, windowData);
            merge(result, windowData);
        }
    }
    
    // Clean up old windows
    _cachedWindows.keySet().removeIf(w -> w < startWindow);
    
    _lastAggregationTime = now;
    return result;
}

private Map<TopicPartition, AggregatedMetrics> aggregateWindow(
        int window, long windowMs, MetricSamplerOptions options) {
    
    long windowStart = window * windowMs;
    long windowEnd = windowStart + windowMs;
    
    // Only query samples in this time range
    List<PartitionMetricSample> samples = 
        _sampleStore.getSamples(windowStart, windowEnd);
    
    // Aggregate samples for this window
    Map<TopicPartition, AggregatedMetrics> windowData = new HashMap<>();
    for (PartitionMetricSample sample : samples) {
        TopicPartition tp = sample.topicPartition();
        windowData.computeIfAbsent(tp, k -> new AggregatedMetrics())
                  .addSample(sample);
    }
    
    return windowData;
}
```

### Memory Management

**Concern:** Caching all windows uses memory

**Solution:** Limit cache size

```java
// Configuration
private static final int MAX_CACHED_WINDOWS = 100;  // ~8 hours at 5min windows

if (_cachedWindows.size() > MAX_CACHED_WINDOWS) {
    // Evict oldest window
    int oldestWindow = _cachedWindows.keySet().stream()
        .min(Integer::compare)
        .orElse(0);
    _cachedWindows.remove(oldestWindow);
}
```

**Memory usage:**
- 1M partitions × 100 windows × 1KB = 100MB (acceptable)

## Example Usage

### Before (Full Aggregation)

```java
// Request ClusterModel at 10:00
aggregate(clusterModel, now=10:00, options)
  → Process windows: [9:40, 9:45, 9:50, 9:55, 10:00]
  → Load 5M samples from SampleStore
  → Aggregate all samples
  → Time: 10 seconds

// Request ClusterModel at 10:02
aggregate(clusterModel, now=10:02, options)
  → Process windows: [9:40, 9:45, 9:50, 9:55, 10:00]  // Same!
  → Load 5M samples from SampleStore  // Same!
  → Aggregate all samples  // Wasted work!
  → Time: 10 seconds
```

### After (Incremental)

```java
// Request ClusterModel at 10:00
aggregate(clusterModel, now=10:00, options)
  → Process windows: [9:40, 9:45, 9:50, 9:55, 10:00]
  → Windows 9:40-9:55: Use cached (instant)
  → Window 10:00: Aggregate (1M samples)
  → Time: 2 seconds  (80% faster!)

// Request ClusterModel at 10:02
aggregate(clusterModel, now=10:02, options)
  → Process windows: [9:40, 9:45, 9:50, 9:55, 10:00]
  → Windows 9:40-10:00: Use cached (instant!)
  → Time: 0.1 seconds  (99% faster!)
```

## Implementation Plan

### Phase 1: Infrastructure (1 week)
- [ ] Add window-based caching to `PartitionMetricSampleAggregator`
- [ ] Implement cache eviction policy
- [ ] Unit tests for cache correctness

### Phase 2: Integration (1 week)
- [ ] Integrate with `LoadMonitor`
- [ ] Add metrics for cache hit rate
- [ ] Performance benchmarks

### Phase 3: Testing (0.5 weeks)
- [ ] Integration tests with time windows
- [ ] Verify correctness vs. full aggregation
- [ ] Load testing

### Phase 4: Rollout (0.5 weeks)
- [ ] Feature flag
- [ ] Monitoring dashboard
- [ ] Documentation

**Total Effort:** 20-25 dev-days

## Backwards Compatibility

**Breaking Changes:** None

**Configuration:**

```properties
# Enable incremental aggregation (default: true)
metrics.aggregation.incremental.enabled=true

# Max cached windows (default: 100)
metrics.aggregation.max.cached.windows=100
```

No API changes. Existing code works identically.

## Alternatives Considered

### Alternative 1: Pre-Aggregate in SampleStore

Store pre-aggregated metrics instead of raw samples:

**Rejected because:**
- Requires SampleStore changes
- Loses flexibility (can't change aggregation logic)
- Harder to debug (raw samples useful)

### Alternative 2: Persistent Cache

Store cache on disk/Redis:

**Rejected because:**
- Adds external dependency
- Complexity not justified
- In-memory cache sufficient

## Open Questions

1. **Q:** What if window boundaries change (config update)?
   **A:** Clear cache, rebuild from scratch

2. **Q:** Memory impact on very large clusters?
   **A:** Configure smaller `max.cached.windows`

3. **Q:** Cache invalidation on metadata changes?
   **A:** Clear cache when topics added/removed

## Success Criteria

- [ ] ClusterModel build time reduced by 80%
- [ ] Cache hit rate > 90% in steady state
- [ ] Memory overhead < 150MB
- [ ] No correctness regressions

---

**Implementation Priority:** P0 (High Impact, Medium Effort)
**Estimated Completion:** Q1 2026
