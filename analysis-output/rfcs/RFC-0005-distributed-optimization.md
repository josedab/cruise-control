# RFC-0005: Distributed Goal Optimization

**Status:** Draft | **Priority:** P1 (Long-term)
**Author:** Analysis Team | **Effort:** 60-90 dev-days
**Created:** 2025-11-20
**Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## Summary

Enable horizontal scaling of Cruise Control by distributing goal optimization across multiple worker nodes, allowing linear scaling to clusters with 100K+ brokers and 10M+ partitions.

## Motivation

**Current Limitation:**

Goal optimization happens on a single Cruise Control instance:

```java
// All goals run on one machine
for (Goal goal : goals) {
    goal.optimize(clusterModel, ...);  // Single-threaded, single machine
}
```

**Scaling Wall:**

| Cluster Size | Optimization Time | Machine Requirements |
|--------------|-------------------|---------------------|
| 10K brokers, 1M partitions | 30-40 min | 8 CPU, 8 GB RAM |
| 50K brokers, 5M partitions | 3-4 hours | 16 CPU, 32 GB RAM |
| 100K brokers, 10M partitions | **10+ hours** | **32 CPU, 64 GB RAM** |

**At 100K+ brokers:**
- Vertical scaling hits limits (cost, availability)
- Single machine is SPOF
- Cannot leverage cloud auto-scaling

**Vision:** Distribute work across N workers, get N× speedup

## Detailed Design

### Architecture: Coordinator-Worker Pattern

```
┌─────────────────────────────────────────────────────────┐
│            Cruise Control Coordinator                   │
│  - LoadMonitor                                          │
│  - Executor                                             │
│  - REST API                                             │
│  - Work Distribution                                    │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Worker 1   │  │  Worker 2   │  │  Worker 3   │
│             │  │             │  │             │
│ Goals 1-5   │  │ Goals 6-10  │  │ Goals 11-15 │
└─────────────┘  └─────────────┘  └─────────────┘
```

**Coordinator Responsibilities:**
- Maintains ClusterModel
- Distributes goals to workers
- Merges results
- Executes proposals

**Worker Responsibilities:**
- Receive ClusterModel snapshot
- Execute assigned goals
- Return modified model

### Phase 1: ClusterModel Distribution

**Challenge:** ClusterModel is 2-3 GB for large clusters

**Solution:** Efficient serialization

```java
public class ClusterModelSnapshot implements Serializable {
    
    // Compressed binary format
    private byte[] _serializedBrokers;
    private byte[] _serializedReplicas;
    private byte[] _serializedLoads;
    
    public static ClusterModelSnapshot create(ClusterModel model) {
        // Serialize with compression
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            // Custom binary format (not Java serialization!)
            writeBrokers(gzip, model.brokers());
            writeReplicas(gzip, model.replicas());
            writeLoads(gzip, model.loads());
        }
        
        return new ClusterModelSnapshot(baos.toByteArray());
    }
    
    public ClusterModel deserialize() {
        // Decompress and rebuild
        ClusterModel model = new ClusterModel();
        try (GZIPInputStream gzip = new GZIPInputStream(
                new ByteArrayInputStream(_serializedData))) {
            readBrokers(gzip, model);
            readReplicas(gzip, model);
            readLoads(gzip, model);
        }
        return model;
    }
}
```

**Optimization:** Send deltas instead of full model

```java
// First worker gets full model
worker1.sendModel(fullModel);  // 2 GB

// Subsequent workers get delta
worker2.sendModel(fullModel);
// After worker1 completes:
ClusterModelDelta delta = diffModels(fullModel, worker1Result);
worker2.sendDelta(delta);  // Only changed replicas, ~100 MB
```

### Phase 2: Goal Assignment Strategy

**Strategy 1: Static Partitioning**

```java
Worker 1: Goals [RackAware, ReplicaCapacity, DiskCapacity]
Worker 2: Goals [CpuCapacity, NetworkInCapacity, NetworkOutCapacity]
Worker 3: Goals [ReplicaDistribution, DiskDistribution, CpuDistribution]
...
```

**Pro:** Simple, predictable
**Con:** Unbalanced (some goals slower than others)

**Strategy 2: Dynamic Work Stealing**

```java
public class WorkQueue {
    private Queue<Goal> _pendingGoals;
    
    public Goal getWork(String workerId) {
        synchronized (_pendingGoals) {
            if (_pendingGoals.isEmpty()) {
                return null;
            }
            Goal goal = _pendingGoals.poll();
            LOG.info("Assigned {} to worker {}", goal, workerId);
            return goal;
        }
    }
}

// Worker loop
while (true) {
    Goal goal = coordinator.getWork(myId);
    if (goal == null) break;  // No more work
    
    ClusterModel result = goal.optimize(model, ...);
    coordinator.submitResult(goal, result);
}
```

**Pro:** Load balancing, fault tolerance
**Con:** More complex coordination

### Phase 3: Result Merging

**Challenge:** Multiple workers modify the same ClusterModel

**Solution:** Sequential merging with conflict resolution

```java
public class DistributedGoalOptimizer {
    
    public OptimizerResult optimizeDistributed(
            ClusterModel initialModel,
            List<Goal> goals,
            List<Worker> workers) {
        
        ClusterModel currentModel = initialModel.copy();
        Set<Goal> optimizedGoals = new HashSet<>();
        
        // Goals still execute in priority order
        // But independent goals can run in parallel
        List<Set<Goal>> goalGroups = groupIndependentGoals(goals);
        
        for (Set<Goal> group : goalGroups) {
            // Distribute group across workers
            Map<Worker, Set<Goal>> assignments = assignGoals(group, workers);
            
            // Send model to each worker
            for (Map.Entry<Worker, Set<Goal>> entry : assignments.entrySet()) {
                Worker worker = entry.getKey();
                Set<Goal> workerGoals = entry.getValue();
                
                worker.optimizeAsync(currentModel, workerGoals, optimizedGoals);
            }
            
            // Wait for all workers to complete
            List<ClusterModel> results = waitForResults(workers);
            
            // Merge results sequentially by goal priority
            currentModel = mergeResults(results, group, currentModel);
            optimizedGoals.addAll(group);
        }
        
        return generateProposals(initialModel, currentModel);
    }
    
    private ClusterModel mergeResults(
            List<ClusterModel> results,
            Set<Goal> goals,
            ClusterModel base) {
        
        ClusterModel merged = base.copy();
        
        // Apply changes from each result in priority order
        for (Goal goal : sortByPriority(goals)) {
            ClusterModel result = findResultForGoal(results, goal);
            
            // Apply only changes made by this goal
            for (Replica replica : result.movedReplicas()) {
                if (wasMovedBy(replica, goal)) {
                    applyMove(merged, replica);
                }
            }
        }
        
        return merged;
    }
}
```

### Phase 4: Worker Communication

**Technology:** gRPC for efficient RPC

```protobuf
// cruise_control_worker.proto
service WorkerService {
    rpc OptimizeGoals(OptimizationRequest) returns (OptimizationResponse);
    rpc GetStatus(StatusRequest) returns (StatusResponse);
}

message OptimizationRequest {
    bytes cluster_model = 1;  // Serialized ClusterModel
    repeated string goals = 2;  // Goal class names
    bytes optimization_options = 3;
}

message OptimizationResponse {
    bytes modified_model = 1;
    repeated string violated_goals = 2;
    int64 optimization_time_ms = 3;
}
```

**Worker implementation:**

```java
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    
    @Override
    public void optimizeGoals(OptimizationRequest request,
                              StreamObserver<OptimizationResponse> responseObserver) {
        try {
            // Deserialize model
            ClusterModel model = ClusterModelSnapshot.deserialize(
                request.getClusterModel().toByteArray());
            
            // Load goals
            List<Goal> goals = loadGoals(request.getGoalsList());
            
            // Optimize
            Set<Goal> violated = new HashSet<>();
            for (Goal goal : goals) {
                boolean wasViolated = goal.optimize(model, ...);
                if (wasViolated) violated.add(goal);
            }
            
            // Serialize result
            byte[] resultModel = ClusterModelSnapshot.create(model).serialize();
            
            OptimizationResponse response = OptimizationResponse.newBuilder()
                .setModifiedModel(ByteString.copyFrom(resultModel))
                .addAllViolatedGoals(violated.stream().map(Goal::name).collect(toList()))
                .setOptimizationTimeMs(System.currentTimeMillis() - start)
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
```

## Scalability Analysis

**Setup:** 100K brokers, 10M partitions, 15 goals

**Single Instance (Current):**
- Time: 10 hours
- Machine: 32 CPU, 64 GB RAM
- Cost: $2.50/hour × 10 hours = $25 per optimization

**Distributed (3 workers):**
- Time: 3.5 hours (2.8× speedup, accounting for coordination)
- Machines: 1 coordinator (8 CPU, 16 GB) + 3 workers (16 CPU, 32 GB each)
- Cost: ($1/hour + $1.50/hour × 3) × 3.5 hours = $19.25
- **22% cost savings + 65% faster**

**Distributed (10 workers):**
- Time: 1.2 hours (8× speedup)
- Cost: ($1/hour + $1.50/hour × 10) × 1.2 hours = $19.20
- **83% faster, similar cost**

## Implementation Plan

### Phase 1: Foundation (4 weeks)
- [ ] ClusterModel serialization/deserialization
- [ ] Worker service gRPC interface
- [ ] Basic coordinator-worker communication
- [ ] Unit tests for serialization

### Phase 2: Distributed Optimization (4 weeks)
- [ ] Goal assignment strategies
- [ ] Result merging logic
- [ ] Conflict detection and resolution
- [ ] Integration tests

### Phase 3: Fault Tolerance (2 weeks)
- [ ] Worker failure detection
- [ ] Work reassignment on failure
- [ ] Coordinator failover (requires RFC-0006)
- [ ] Chaos testing

### Phase 4: Performance Optimization (2 weeks)
- [ ] ClusterModel compression
- [ ] Delta updates
- [ ] Connection pooling
- [ ] Benchmarking

### Phase 5: Production Readiness (2 weeks)
- [ ] Monitoring and metrics
- [ ] Auto-scaling integration
- [ ] Configuration and deployment guides
- [ ] Documentation

**Total Effort:** 60-90 dev-days (14 weeks with 1-2 engineers)

## Backwards Compatibility

**Breaking Changes:** None (new capability, not replacement)

**Configuration:**

```properties
# Enable distributed optimization
distributed.optimization.enabled=false

# Worker endpoints (comma-separated)
distributed.optimization.workers=worker1:9091,worker2:9091,worker3:9091

# Goal assignment strategy: STATIC, DYNAMIC
distributed.optimization.assignment.strategy=DYNAMIC

# Worker timeout (ms)
distributed.optimization.worker.timeout.ms=600000
```

Single-instance mode remains default and fully supported.

## Alternatives Considered

### Alternative 1: Apache Spark for Distribution

Use Spark's distributed computation framework:

**Rejected because:**
- Heavy dependency (entire Spark runtime)
- Overkill for our use case
- Harder to integrate with existing code

### Alternative 2: Kafka Streams for Coordination

Use Kafka Streams for worker coordination:

**Rejected because:**
- Kafka Streams designed for stream processing, not RPC
- State stores not ideal for large ClusterModels
- Adds complexity

### Alternative 3: MapReduce-Style Distribution

Partition brokers, optimize subsets independently:

**Rejected because:**
- Violates goal dependencies (cross-broker constraints)
- Hard to merge results correctly
- Goals inherently holistic

## Open Questions

1. **Q:** Network bandwidth requirements?
   **A:** 2-3 GB per worker initially, then ~100 MB deltas. Needs 10 Gbps network.

2. **Q:** How to handle worker failures mid-optimization?
   **A:** Reassign failed worker's goals to healthy workers. Optimization restarts from last checkpoint.

3. **Q:** Can we distribute LoadMonitor too?
   **A:** Potentially, but less benefit (already fast). Focus on optimization first.

4. **Q:** Coordinator SPOF?
   **A:** Yes. Addressed in RFC-0006 (Leader Election for HA).

## Success Criteria

- [ ] Linear scalability: N workers → N× speedup (±20%)
- [ ] Fault tolerance: Survive 1 worker failure
- [ ] Cost efficiency: Lower cost per optimization at scale
- [ ] Correctness: Results identical to single-instance mode

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Network bottleneck | Medium | Use compression, delta updates |
| Result merge complexity | High | Extensive testing, formal verification |
| Worker coordination overhead | Medium | Efficient serialization, connection pooling |
| Operational complexity | High | Comprehensive monitoring, auto-scaling |

---

**Implementation Priority:** P1 (High Impact, Very High Effort)
**Target:** Q3-Q4 2026
**Dependencies:** None (RFC-0006 optional for full HA)
**Risk Level:** High (significant architectural change)

---

*This RFC enables Cruise Control to scale to 100K+ broker clusters, the next frontier for Apache Kafka deployments.*
