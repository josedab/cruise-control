# RFC-0004: Smarter Proposal Caching

**Status:** Draft | **Priority:** P0 (Quick Win)
**Author:** Analysis Team | **Effort:** 5-7 dev-days
**Created:** 2025-11-20

## Summary

Improve proposal cache hit rate from <10% to >90% by implementing partial invalidation instead of full cache invalidation.

## Motivation

Current cache invalidation is too aggressive:

```java
// Any change invalidates entire cache
if (clusterModel.generation() != _cachedGeneration) {
    _cachedProposals = null;  // Throw away all work!
}
```

**Scenarios where cache is unnecessarily invalidated:**
- Metrics updated (proposals topology unchanged)
- Single broker added (affects <5% of partitions)
- Topic created with 1 partition (affects 0.0001% of cluster)

## Detailed Design

### Change Types

```java
enum ClusterChangeType {
    METRICS_ONLY,           // Just metric updates
    SINGLE_BROKER_ADDED,    // One broker joined
    SINGLE_BROKER_REMOVED,  // One broker left
    TOPIC_ADDED,            // New topic created
    PARTITION_ADDED,        // Partition count increased
    MAJOR_TOPOLOGY_CHANGE   // Multiple brokers/topics
}
```

### Smart Cache Update

```java
public class SmartProposalCache {
    
    public Set<ExecutionProposal> getProposals(ClusterModel currentModel) {
        ClusterChangeType changeType = detectChangeType(
            _cachedModel, currentModel);
        
        switch (changeType) {
            case METRICS_ONLY:
                // Just update proposal estimates
                return updateProposalEstimates(_cachedProposals, currentModel);
            
            case SINGLE_BROKER_ADDED:
                // Only generate proposals for new broker
                return addBrokerProposals(
                    _cachedProposals, currentModel, newBrokerId);
            
            case TOPIC_ADDED:
                // Cache still valid (new topic partitions already balanced)
                return _cachedProposals;
            
            default:
                // Full regeneration
                return regenerateProposals(currentModel);
        }
    }
}
```

## Implementation Plan

**Week 1:**
- Implement change detection
- Add partial update logic
- Unit tests

**Total:** 5-7 dev-days

## Success Criteria

- Cache hit rate > 90%
- Proposal generation time < 1s (cached)

---

**Impact:** High | **Effort:** Low | **Start:** Immediately
