# RFC-0001: Parallel Goal Execution

**Status:** Draft
**Author:** Analysis Team
**Created:** 2025-11-20
**Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## Summary

Optimize goal execution by running independent goals in parallel, reducing total optimization time by 40-60% for typical goal sets on large clusters.

## Motivation

**Current Problem:**

Goals execute serially in priority order:

```java
for (Goal goal : goals) {  // Serial loop - 15 iterations
    goal.optimize(clusterModel, optimizedGoals, options);  // 2-10 minutes each
}
```

For a large cluster (10K brokers, 1M partitions) with 15 goals:
- Total time: 30-40 minutes
- CPU utilization: ~25% (single-threaded)
- User wait time: unacceptable for interactive use

**Why This Matters:**

1. **Interactive rebalancing:** Users wait 30+ minutes for proposals
2. **Self-healing delays:** Anomaly fixes take too long
3. **Resource waste:** Multi-core machines underutilized

## Detailed Design

### Goal Dependency Graph

Not all goals depend on all previous goals. Example:

```
RackAwareGoal                 (no dependencies)
    ↓
ReplicaCapacityGoal          (depends on RackAware)
    ↓
DiskCapacityGoal             (depends on ReplicaCapacity)
    ↓
    ├─→ ReplicaDistributionGoal     (independent of CPU/Network goals)
    └─→ CpuUsageDistributionGoal    (independent of Replica goal)
```

`ReplicaDistributionGoal` and `CpuUsageDistributionGoal` can run in parallel!

### Implementation

#### Step 1: Build Dependency Graph

```java
public class GoalDependencyAnalyzer {
    
    public List<Set<Goal>> buildExecutionGroups(List<Goal> goals) {
        List<Set<Goal>> groups = new ArrayList<>();
        Set<Goal> processed = new HashSet<>();
        
        for (Goal goal : goals) {
            if (processed.contains(goal)) continue;
            
            // Find all goals that can run with this goal
            Set<Goal> parallelGroup = new HashSet<>();
            parallelGroup.add(goal);
            
            for (Goal candidate : goals) {
                if (processed.contains(candidate)) continue;
                if (canRunInParallel(goal, candidate, processed)) {
                    parallelGroup.add(candidate);
                }
            }
            
            groups.add(parallelGroup);
            processed.addAll(parallelGroup);
        }
        
        return groups;
    }
    
    private boolean canRunInParallel(Goal g1, Goal g2, Set<Goal> processed) {
        // Goals can run in parallel if:
        // 1. They don't modify the same resources
        // 2. They have same dependencies (all in 'processed')
        // 3. Neither depends on the other
        
        Set<Resource> resources1 = g1.modifiedResources();
        Set<Resource> resources2 = g2.modifiedResources();
        
        return Collections.disjoint(resources1, resources2)
            && haveSameDependencies(g1, g2, processed);
    }
}
```

#### Step 2: Parallel Execution with Conflict Detection

```java
public class ParallelGoalOptimizer {
    
    private final ExecutorService _executorService;
    
    public OptimizerResult optimizations(ClusterModel clusterModel,
                                         List<Goal> goals,
                                         OptimizationOptions options) {
        // Build execution groups
        List<Set<Goal>> groups = new GoalDependencyAnalyzer()
            .buildExecutionGroups(goals);
        
        Set<Goal> optimizedGoals = new HashSet<>();
        Map<Goal, ClusterModel> results = new ConcurrentHashMap<>();
        
        for (Set<Goal> group : groups) {
            // Each goal gets its own copy of cluster model
            Map<Goal, ClusterModel> models = new HashMap<>();
            for (Goal goal : group) {
                models.put(goal, clusterModel.copy());
            }
            
            // Execute goals in parallel
            List<Future<Boolean>> futures = group.stream()
                .map(goal -> _executorService.submit(() -> {
                    ClusterModel model = models.get(goal);
                    return goal.optimize(model, optimizedGoals, options);
                }))
                .collect(Collectors.toList());
            
            // Wait for all to complete
            for (Future<Boolean> future : futures) {
                future.get();  // Blocks until complete
            }
            
            // Merge results and detect conflicts
            clusterModel = mergeModels(models.values(), options);
            results.putAll(models);
            optimizedGoals.addAll(group);
        }
        
        return buildOptimizerResult(clusterModel, results);
    }
    
    private ClusterModel mergeModels(Collection<ClusterModel> models,
                                      OptimizationOptions options) {
        // Start with first model
        ClusterModel merged = models.iterator().next().copy();
        
        // Apply changes from other models
        for (ClusterModel model : models) {
            // For each replica that moved
            for (Replica replica : model.movedReplicas()) {
                Broker newBroker = model.broker(replica.broker().id());
                
                // Check for conflicts (two goals moved same replica)
                if (merged.hasReplica(replica) 
                    && merged.replicaBroker(replica) != newBroker) {
                    // Conflict! Resolve by goal priority
                    resolveConflict(merged, model, replica, options);
                } else {
                    // No conflict, apply move
                    merged.relocateReplica(replica, newBroker);
                }
            }
        }
        
        return merged;
    }
}
```

### Conflict Resolution

When two parallel goals move the same replica:

1. **Priority-based:** Higher priority goal wins
2. **Validation:** Loser's goal re-validates its changes
3. **Rollback:** If validation fails, rollback conflicting changes

## Example Usage

**Before:**

```
Goal execution:
RackAwareGoal:              120s  ━━━━━━━━━━━━
ReplicaCapacityGoal:         60s  ━━━━━━
DiskCapacityGoal:           90s  ━━━━━━━━━
CpuCapacityGoal:            80s  ━━━━━━━━
ReplicaDistributionGoal:     50s  ━━━━━
CpuUsageDistributionGoal:    40s  ━━━━
...
Total:                      600s
```

**After:**

```
Group 1 (serial):
RackAwareGoal:              120s  ━━━━━━━━━━━━

Group 2 (serial):
ReplicaCapacityGoal:         60s  ━━━━━━

Group 3 (PARALLEL):
DiskCapacityGoal:           90s  ━━━━━━━━━
CpuCapacityGoal:            80s  ━━━━━━━━
NetworkInCapacityGoal:      70s  ━━━━━━━

Group 4 (PARALLEL):
ReplicaDistributionGoal:     50s  ━━━━━
CpuUsageDistributionGoal:    40s  ━━━━
DiskUsageDistributionGoal:   45s  ━━━━━

Total:                      360s  (40% faster!)
```

## Implementation Plan

### Phase 1: Foundation (2 weeks)
- [ ] Implement `GoalDependencyAnalyzer`
- [ ] Add `Goal.modifiedResources()` method
- [ ] Unit tests for dependency detection

### Phase 2: Parallel Execution (2 weeks)
- [ ] Implement `ParallelGoalOptimizer`
- [ ] Add ClusterModel merge logic
- [ ] Conflict detection and resolution

### Phase 3: Testing (1 week)
- [ ] Integration tests with real goals
- [ ] Performance benchmarks
- [ ] Validate correctness against serial execution

### Phase 4: Rollout (1 week)
- [ ] Feature flag for gradual rollout
- [ ] Monitoring and metrics
- [ ] Documentation

**Total Effort:** 30-40 dev-days

## Backwards Compatibility

**Breaking Changes:** None

**Configuration:**

```properties
# Enable parallel goal execution (default: false)
goal.optimizer.parallel.enabled=true

# Max parallel goals (default: num CPUs)
goal.optimizer.parallel.threads=8
```

Existing goals work without modification. Users opt-in via config.

## Alternatives Considered

### Alternative 1: Goal Fusion

Merge multiple goals into single "super goals":

```java
public class ResourceCapacityGoal extends AbstractGoal {
    // Combines: DiskCapacityGoal, CpuCapacityGoal, NetworkInCapacityGoal
}
```

**Rejected because:**
- Reduces modularity
- Harder to maintain
- Doesn't help with distribution goals

### Alternative 2: Async Goal Execution

Goals return `Future<Boolean>` instead of blocking:

**Rejected because:**
- Requires rewriting all 28 goals
- Breaking change
- Doesn't solve merge problem

## Open Questions

1. **Q:** What if conflict rate is high?
   **A:** Fallback to serial execution for that group

2. **Q:** How to handle custom goals?
   **A:** Default to conservative dependencies (run serially)

3. **Q:** Memory impact of parallel models?
   **A:** Limit to 4-8 parallel goals maximum

## Success Criteria

- [ ] Optimization time reduced by 40%+ on large clusters
- [ ] No correctness regressions (proposals identical to serial)
- [ ] CPU utilization increases to 60%+
- [ ] Memory overhead < 20%

---

**Implementation Priority:** P0 (High Impact, High Effort)
**Estimated Completion:** Q2 2026
