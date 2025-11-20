# RFC-0011: Multi-Cluster Management

**Status:** Proposed
**Created:** 2025-11-20
**Authors:** Analysis Team
**Commit Base:** [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

---

## Executive Summary

Cruise Control currently manages **one Kafka cluster per instance**. Large enterprises operate **dozens to hundreds of Kafka clusters** (dev, staging, prod, regional, by-team), each requiring separate Cruise Control deployment. This RFC proposes **multi-cluster management** allowing a single Cruise Control instance to monitor and optimize **multiple Kafka clusters** from a unified dashboard.

**Impact:** CRITICAL for enterprises - Reduces operational overhead by 90%, unified visibility
**Effort:** 50-70 developer-days (10-14 weeks)
**Risk:** Medium (requires architectural changes but backward compatible)

---

## Problem Statement

### Current Multi-Cluster Pain Points

#### 1. **Operational Overhead**

**Example Enterprise Setup:**
```
Company XYZ has:
- 3 production clusters (US-East, US-West, EU)
- 5 staging clusters (per product line)
- 10 development clusters (per team)
- 2 DR clusters
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total: 20 Kafka clusters

Current requirement:
20 Cruise Control instances × (deployment + monitoring + maintenance)
```

**Operational Costs:**
- 20 separate deployments to maintain
- 20 sets of configurations to manage
- 20 separate UIs to monitor
- 20 separate upgrade cycles
- **Estimated: 2-3 FTE just for Cruise Control operations**

#### 2. **No Cross-Cluster Visibility**

**Current State:**
- Must open 20 different browser tabs/URLs
- No unified dashboard
- Can't compare cluster health at a glance
- No centralized alerting

**Example Scenario:**
```
Question: "Which clusters are currently rebalancing?"
Current Answer: Check 20 separate UIs manually (10+ minutes)
Desired Answer: Single dashboard showing all (5 seconds)
```

#### 3. **Resource Inefficiency**

**Per-Instance Resource Usage:**
- Memory: 4-8GB per Cruise Control instance
- CPU: 2-4 cores per instance
- Storage: 10GB per instance

**Total for 20 clusters:**
- Memory: 80-160GB
- CPU: 40-80 cores
- Storage: 200GB

**Waste:** Small clusters (dev/staging) don't need full resources but get them anyway

#### 4. **Configuration Duplication**

**Common Configuration:**
```properties
# Must maintain separately in 20 config files:
goals=RackAwareGoal,ReplicaCapacityGoal,...  # Same for all
anomaly.detection.goals=...                   # Same for all
self.healing.enabled=true                     # Same for all

# Only cluster-specific differences:
bootstrap.servers=cluster1:9092  # Different per cluster
zookeeper.connect=zk1:2181       # Different per cluster
```

**Problem:** Change to goals list requires updating 20 configuration files

#### 5. **Limited Multi-Cluster Operations**

**Desired Operations:**
- Migrate topic between clusters
- Compare optimization proposals across clusters
- Coordinate maintenance windows across clusters
- Aggregate metrics across all clusters

**Current State:** Not possible without custom tooling

---

## Real-World Use Cases

### Use Case 1: Multi-Region Deployment

**Scenario:** Global company with regional Kafka clusters

```
┌─────────────────────────────────────────────────┐
│         Cruise Control (Multi-Cluster)          │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ US-East  │  │ US-West  │  │  EU-West │     │
│  │ Cluster  │  │ Cluster  │  │  Cluster │     │
│  └──────────┘  └──────────┘  └──────────┘     │
└─────────────────────────────────────────────────┘

Benefits:
- Single pane of glass for global infrastructure
- Consistent optimization policies across regions
- Compare cluster health across regions
- Coordinate rolling upgrades
```

### Use Case 2: Multi-Tenant Platform

**Scenario:** Platform team manages Kafka clusters for multiple product teams

```
Platform Team Dashboard:
┌────────────────────────────────────────────┐
│ Team Alpha Cluster    ✓ Healthy            │
│ Team Beta Cluster     ⚠ Rebalancing        │
│ Team Gamma Cluster    ✓ Healthy            │
│ Team Delta Cluster    ❌ Goal Violation     │
└────────────────────────────────────────────┘

Benefits:
- Platform team can see all clusters at once
- Proactive identification of issues
- Standardized configurations across teams
```

### Use Case 3: Environment Management

**Scenario:** Dev → Staging → Prod lifecycle

```
All Environments in One Place:
┌────────────────────────────────────────────┐
│ Production Cluster    ✓ Healthy (50 nodes) │
│ Staging Cluster       ✓ Healthy (10 nodes) │
│ Dev Cluster           ✓ Healthy (3 nodes)  │
└────────────────────────────────────────────┘

Benefits:
- Ensure prod and staging have same goals
- Test configuration changes in dev first
- Promote configs from dev → staging → prod
```

---

## Proposed Solution

### Architecture: Multi-Cluster Controller

```
┌────────────────────────────────────────────────────────┐
│               Cruise Control (Enhanced)                │
│                                                         │
│  ┌──────────────────────────────────────────────────┐ │
│  │        Multi-Cluster Coordinator                  │ │
│  │  - Cluster Registry                               │ │
│  │  - Unified Configuration                          │ │
│  │  - Cross-Cluster Operations                       │ │
│  └──────────────────────────────────────────────────┘ │
│              │              │              │            │
│              ▼              ▼              ▼            │
│  ┌────────────────┐ ┌────────────────┐ ┌─────────┐   │
│  │ Cluster Context│ │ Cluster Context│ │ Cluster │   │
│  │   "US-East"    │ │   "US-West"    │ │ Context │   │
│  │                │ │                │ │"EU-West"│   │
│  │ LoadMonitor    │ │ LoadMonitor    │ │LoadMon  │   │
│  │ GoalOptimizer  │ │ GoalOptimizer  │ │GoalOpt  │   │
│  │ Executor       │ │ Executor       │ │Executor │   │
│  │ Anomaly Det.   │ │ Anomaly Det.   │ │Anomaly  │   │
│  └────────┬───────┘ └────────┬───────┘ └────┬────┘   │
└───────────┼──────────────────┼──────────────┼─────────┘
            │                  │              │
            ▼                  ▼              ▼
      ┌──────────┐       ┌──────────┐   ┌──────────┐
      │  Kafka   │       │  Kafka   │   │  Kafka   │
      │ Cluster 1│       │ Cluster 2│   │ Cluster 3│
      └──────────┘       └──────────┘   └──────────┘
```

### Key Components

#### 1. Cluster Registry

**Purpose:** Register and manage multiple cluster configurations

**Configuration: `clusters.properties`**

```properties
# Cluster definitions
clusters=us-east,us-west,eu-west

# US-East Cluster
cluster.us-east.bootstrap.servers=kafka1-us-east:9092,kafka2-us-east:9092
cluster.us-east.zookeeper.connect=zk1-us-east:2181,zk2-us-east:2181
cluster.us-east.display.name=US East Production
cluster.us-east.region=us-east-1
cluster.us-east.environment=production
cluster.us-east.enabled=true

# US-West Cluster
cluster.us-west.bootstrap.servers=kafka1-us-west:9092,kafka2-us-west:9092
cluster.us-west.zookeeper.connect=zk1-us-west:2181,zk2-us-west:2181
cluster.us-west.display.name=US West Production
cluster.us-west.region=us-west-2
cluster.us-west.environment=production
cluster.us-west.enabled=true

# EU-West Cluster
cluster.eu-west.bootstrap.servers=kafka1-eu-west:9092,kafka2-eu-west:9092
cluster.eu-west.zookeeper.connect=zk1-eu-west:2181,zk2-eu-west:2181
cluster.eu-west.display.name=EU West Production
cluster.eu-west.region=eu-west-1
cluster.eu-west.environment=production
cluster.eu-west.enabled=true

# Shared Configuration (applies to all clusters unless overridden)
default.goals=RackAwareGoal,ReplicaCapacityGoal,DiskCapacityGoal,...
default.anomaly.detection.goals=RackAwareGoal,ReplicaCapacityGoal
default.self.healing.enabled=true

# Per-cluster overrides (optional)
cluster.eu-west.goals=RackAwareGoal,MinTopicLeadersPerBrokerGoal,...
```

**Java API:**

```java
/**
 * Registry of managed Kafka clusters.
 */
public class ClusterRegistry {
    private final Map<String, ClusterConfig> _clusters = new ConcurrentHashMap<>();

    /**
     * Registers a new cluster.
     *
     * @param clusterId unique identifier for the cluster
     * @param config cluster-specific configuration
     */
    public void registerCluster(String clusterId, ClusterConfig config) {
        _clusters.put(clusterId, config);
    }

    /**
     * Gets configuration for a specific cluster.
     */
    public ClusterConfig getCluster(String clusterId) {
        return _clusters.get(clusterId);
    }

    /**
     * Lists all registered clusters.
     */
    public Set<String> listClusters() {
        return _clusters.keySet();
    }

    /**
     * Filters clusters by environment (prod, staging, dev).
     */
    public List<ClusterConfig> getClustersByEnvironment(String environment) {
        return _clusters.values().stream()
            .filter(c -> c.environment().equals(environment))
            .collect(Collectors.toList());
    }

    /**
     * Filters clusters by region.
     */
    public List<ClusterConfig> getClustersByRegion(String region) {
        return _clusters.values().stream()
            .filter(c -> c.region().equals(region))
            .collect(Collectors.toList());
    }
}
```

#### 2. Cluster Context (Isolated State)

**Purpose:** Maintain separate state for each cluster

```java
/**
 * Encapsulates all state for a single Kafka cluster.
 *
 * <p>Each cluster has its own isolated:
 * - LoadMonitor
 * - GoalOptimizer
 * - Executor
 * - AnomalyDetector
 * - Metrics
 */
public class ClusterContext {
    private final String _clusterId;
    private final ClusterConfig _config;

    // Core components (isolated per cluster)
    private final LoadMonitor _loadMonitor;
    private final GoalOptimizer _goalOptimizer;
    private final Executor _executor;
    private final AnomalyDetectorManager _anomalyDetector;

    // Cluster state
    private volatile ClusterModel _clusterModel;
    private volatile ExecutorState _executorState;

    public ClusterContext(String clusterId, ClusterConfig config) {
        this._clusterId = clusterId;
        this._config = config;

        // Initialize components with cluster-specific config
        this._loadMonitor = new LoadMonitor(
            config.toKafkaConfig(),
            new Time(),
            new MetricRegistry()
        );

        this._goalOptimizer = new GoalOptimizer(
            config,
            config.goals(),
            new Time()
        );

        this._executor = new Executor(
            config,
            new Time(),
            new MetricRegistry()
        );

        this._anomalyDetector = new AnomalyDetectorManager(
            config,
            _loadMonitor
        );
    }

    /**
     * Starts monitoring and anomaly detection for this cluster.
     */
    public void start() {
        _loadMonitor.startUp();
        _anomalyDetector.startDetection();
    }

    /**
     * Gets the current cluster model.
     */
    public ClusterModel clusterModel() {
        return _clusterModel;
    }

    /**
     * Executes optimization proposals for this cluster.
     */
    public void executeProposals(Collection<ExecutionProposal> proposals) {
        _executor.executeProposals(proposals, new ExecutionOptions());
    }

    // ... other cluster-specific operations
}
```

#### 3. Multi-Cluster Coordinator

**Purpose:** Coordinate operations across clusters

```java
/**
 * Coordinates operations across multiple Kafka clusters.
 */
public class MultiClusterCoordinator {
    private final ClusterRegistry _registry;
    private final Map<String, ClusterContext> _contexts = new ConcurrentHashMap<>();

    /**
     * Initializes all registered clusters.
     */
    public void initialize() {
        for (String clusterId : _registry.listClusters()) {
            ClusterConfig config = _registry.getCluster(clusterId);

            if (config.enabled()) {
                ClusterContext context = new ClusterContext(clusterId, config);
                context.start();
                _contexts.put(clusterId, context);
            }
        }
    }

    /**
     * Gets cluster context for specific cluster.
     */
    public ClusterContext getContext(String clusterId) {
        return _contexts.get(clusterId);
    }

    /**
     * Executes operation on all clusters in parallel.
     *
     * @param operation the operation to perform
     * @return results keyed by cluster ID
     */
    public <T> Map<String, T> executeOnAll(
            Function<ClusterContext, T> operation) {

        return _contexts.entrySet().parallelStream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> operation.apply(entry.getValue())
            ));
    }

    /**
     * Aggregates metrics across all clusters.
     */
    public AggregatedMetrics aggregateMetrics() {
        Map<String, ClusterMetrics> perCluster = executeOnAll(
            context -> context.getMetrics()
        );

        return new AggregatedMetrics(perCluster);
    }

    /**
     * Gets health status for all clusters.
     */
    public Map<String, ClusterHealth> getAllHealth() {
        return executeOnAll(context -> {
            return new ClusterHealth(
                context.clusterModel().brokerCount(),
                context.clusterModel().partitionCount(),
                context.executorState(),
                context.hasGoalViolations()
            );
        });
    }
}
```

---

## Enhanced REST API

### Cluster-Scoped Endpoints

**All existing endpoints accept optional `cluster` parameter:**

```
# Single-cluster operation (existing behavior)
POST /rebalance?goals=RackAwareGoal&dryrun=false

# Multi-cluster: specify cluster
POST /rebalance?cluster=us-east&goals=RackAwareGoal&dryrun=false

# Multi-cluster: operate on ALL clusters
POST /rebalance?cluster=*&goals=RackAwareGoal&dryrun=false
```

### New Multi-Cluster Endpoints

#### 1. List Clusters

```
GET /clusters

Response:
{
  "clusters": [
    {
      "id": "us-east",
      "displayName": "US East Production",
      "region": "us-east-1",
      "environment": "production",
      "bootstrapServers": "kafka1-us-east:9092,...",
      "brokerCount": 50,
      "partitionCount": 10000,
      "status": "HEALTHY",
      "executorState": "NO_TASK_IN_PROGRESS"
    },
    {
      "id": "us-west",
      "displayName": "US West Production",
      "region": "us-west-2",
      "environment": "production",
      "bootstrapServers": "kafka1-us-west:9092,...",
      "brokerCount": 50,
      "partitionCount": 8000,
      "status": "HEALTHY",
      "executorState": "INTER_BROKER_REPLICA_MOVEMENT"
    }
  ]
}
```

#### 2. Cluster Health Dashboard

```
GET /clusters/health

Response:
{
  "summary": {
    "totalClusters": 20,
    "healthy": 18,
    "degraded": 1,
    "critical": 1
  },
  "clusters": {
    "us-east": {
      "status": "HEALTHY",
      "brokers": 50,
      "goalViolations": 0,
      "ongoingExecutions": 0
    },
    "us-west": {
      "status": "DEGRADED",
      "brokers": 50,
      "goalViolations": 2,
      "ongoingExecutions": 1
    },
    "eu-west": {
      "status": "CRITICAL",
      "brokers": 48,
      "goalViolations": 15,
      "ongoingExecutions": 0,
      "error": "2 brokers down"
    }
  }
}
```

#### 3. Aggregated Metrics

```
GET /clusters/metrics

Response:
{
  "aggregated": {
    "totalBrokers": 1000,
    "totalPartitions": 200000,
    "totalDataGB": 50000,
    "ongoingRebalances": 2
  },
  "perCluster": {
    "us-east": {
      "brokers": 50,
      "partitions": 10000,
      "dataGB": 2500
    },
    ...
  }
}
```

#### 4. Cross-Cluster Comparison

```
GET /clusters/compare?clusters=us-east,us-west&metric=cpu_usage

Response:
{
  "metric": "cpu_usage",
  "clusters": {
    "us-east": {
      "average": 45.2,
      "max": 78.3,
      "min": 12.1
    },
    "us-west": {
      "average": 52.7,
      "max": 89.1,
      "min": 18.4
    }
  }
}
```

---

## Enhanced UI: Multi-Cluster Dashboard

### Main Dashboard View

```
╔══════════════════════════════════════════════════════════════╗
║              Cruise Control - Multi-Cluster                  ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Overview:  20 Clusters  |  18 Healthy  |  2 Issues          ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ Filter:  [All ▼] Environment  [All ▼] Region           │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐║
║  │ US-East Production          ✓ HEALTHY          50 nodes ││
║  │ └─ No goal violations       └─ No executions           ││
║  │    [View] [Rebalance] [State]                          ││
║  ├─────────────────────────────────────────────────────────┤║
║  │ US-West Production          ⚠ DEGRADED         50 nodes ││
║  │ └─ 2 goal violations        └─ Rebalancing (30% done)  ││
║  │    [View] [Stop] [State]                               ││
║  ├─────────────────────────────────────────────────────────┤║
║  │ EU-West Production          ❌ CRITICAL         48 nodes ││
║  │ └─ 15 goal violations       └─ 2 brokers down!         ││
║  │    [View] [Rebalance] [Alerts]                         ││
║  ├─────────────────────────────────────────────────────────┤║
║  │ Staging Cluster             ✓ HEALTHY          10 nodes ││
║  │ Dev Cluster                 ✓ HEALTHY           3 nodes ││
║  │ ... (show more)                                         ││
║  └─────────────────────────────────────────────────────────┘║
║                                                               ║
║  [Batch Operations ▼]  [Compare Clusters]  [Settings]       ║
╚══════════════════════════════════════════════════════════════╝
```

### Batch Operations UI

```
╔══════════════════════════════════════════════════════════════╗
║                Batch Rebalance Operation                      ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Select Clusters:                                            ║
║  ☑ US-East Production                                        ║
║  ☑ US-West Production                                        ║
║  ☑ EU-West Production                                        ║
║  ☐ Staging Cluster                                           ║
║  ☐ Dev Cluster                                               ║
║                                                               ║
║  Goals: [RackAwareGoal, ReplicaCapacityGoal, ...]           ║
║                                                               ║
║  Options:                                                     ║
║  ☑ Dry run first                                             ║
║  ☑ Execute sequentially (one at a time)                      ║
║  ☐ Execute in parallel                                       ║
║                                                               ║
║  [Preview Proposals]  [Execute Batch]  [Cancel]             ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Configuration Management

### Hierarchical Configuration

```
Global Config → Environment Config → Cluster Config
   (default)       (prod/staging/dev)    (specific cluster)
```

**Example:**

```properties
# global-defaults.properties
goals=RackAwareGoal,ReplicaCapacityGoal,...
anomaly.detection.enabled=true
self.healing.enabled=false

# environment-production.properties
self.healing.enabled=true  # Override: enable for prod only
max.num.cluster.movements=50

# environment-staging.properties
max.num.cluster.movements=20  # Override: more conservative

# cluster-us-east.properties
# Specific overrides for US-East only
num.concurrent.partition.movements.per.broker=5
```

---

## Implementation Plan

### Phase 1: Core Multi-Cluster Foundation (Week 1-4)

**Tasks:**
1. Create `ClusterRegistry` class
2. Create `ClusterContext` class (encapsulate existing components)
3. Create `MultiClusterCoordinator` class
4. Add cluster-scoped configuration loading
5. Refactor `KafkaCruiseControl` to support multiple contexts

**Deliverables:**
- Multi-cluster architecture working
- Can register and manage multiple clusters
- Basic REST API with `cluster` parameter

### Phase 2: REST API Enhancements (Week 5-6)

**Tasks:**
1. Add `/clusters` endpoint
2. Add `/clusters/health` endpoint
3. Add `/clusters/metrics` endpoint
4. Add cluster parameter to all existing endpoints
5. Support wildcard `cluster=*` for batch operations

**Deliverables:**
- Complete multi-cluster REST API
- Backward compatible (single cluster still works)

### Phase 3: UI Dashboard (Week 7-10)

**Tasks:**
1. Create multi-cluster dashboard view
2. Add cluster filter/search
3. Add batch operations UI
4. Add cross-cluster comparison views
5. Update existing single-cluster views to work in multi-cluster mode

**Deliverables:**
- Functional multi-cluster UI
- Intuitive navigation between clusters

### Phase 4: Advanced Features (Week 11-14)

**Tasks:**
1. Dynamic cluster registration (add/remove without restart)
2. Cluster templates (quickly add similar clusters)
3. Cross-cluster data migration tools
4. Advanced batch operations (conditional execution)
5. Multi-cluster alerting

**Deliverables:**
- Production-ready multi-cluster management
- Advanced operational features

---

## Migration Path

### Backward Compatibility

**Single-cluster deployments work unchanged:**

```properties
# Old single-cluster config (still works!)
bootstrap.servers=localhost:9092
zookeeper.connect=localhost:2181
goals=RackAwareGoal,...
```

**Internally treated as cluster ID: `default`**

### Gradual Migration

**Step 1: Add second cluster to existing deployment**

```properties
# Keep existing config
bootstrap.servers=localhost:9092  # Becomes "default" cluster
...

# Add second cluster
clusters=default,us-west
cluster.us-west.bootstrap.servers=kafka-west:9092
cluster.us-west.zookeeper.connect=zk-west:2181
```

**Step 2: Migrate to explicit multi-cluster config**

```properties
clusters=us-east,us-west

cluster.us-east.bootstrap.servers=kafka-east:9092
cluster.us-east.zookeeper.connect=zk-east:2181

cluster.us-west.bootstrap.servers=kafka-west:9092
cluster.us-west.zookeeper.connect=zk-west:2181
```

---

## Resource Efficiency

### Shared vs Isolated Resources

| Component | Sharing Strategy | Benefit |
|-----------|------------------|---------|
| Thread pools | Shared across clusters | 50% reduction in threads |
| HTTP server | Single server, cluster-scoped routes | 1 port instead of N |
| Metrics registry | Separated by cluster ID | Clean metric namespacing |
| LoadMonitor state | Isolated per cluster | Safety, no crosstalk |

### Memory Efficiency

**Single-cluster mode:**
- Base: 1GB
- ClusterModel: 2GB (1M partitions)
- Total: 3GB

**Multi-cluster mode (10 clusters):**
- Base: 1GB (shared)
- ClusterModels: 10 × 2GB = 20GB (isolated)
- Total: 21GB

**Savings:** 21GB vs 30GB (10 separate instances) = **30% reduction**

---

## Security Considerations

### Multi-Tenancy Isolation

**Challenge:** Prevent cross-cluster data leakage

**Solution: Strict Context Isolation**

```java
public class ClusterContext {
    // Each context has isolated Kafka connections
    private final AdminClient _adminClient;
    private final KafkaConsumer _metricsConsumer;

    // No shared mutable state between contexts
    private volatile ClusterModel _clusterModel;  // Isolated

    // Prevent accidental cross-cluster operations
    public void executeProposals(Collection<ExecutionProposal> proposals) {
        // Validate proposals are for THIS cluster only
        for (ExecutionProposal proposal : proposals) {
            if (!proposal.clusterId().equals(this._clusterId)) {
                throw new SecurityException(
                    "Attempt to execute proposal for different cluster!"
                );
            }
        }
        // ... execute ...
    }
}
```

### Access Control

**Cluster-level RBAC:**

```
User Role: "us-east-operator"
Permissions:
- cluster:us-east:read
- cluster:us-east:rebalance
- cluster:us-east:stop-execution

Denied:
- cluster:us-west:*  (cannot access other clusters)
```

---

## Testing Strategy

### Unit Tests

```java
@Test
public void testMultiClusterRegistration() {
    ClusterRegistry registry = new ClusterRegistry();

    registry.registerCluster("cluster1", config1);
    registry.registerCluster("cluster2", config2);

    assertEquals(2, registry.listClusters().size());
    assertTrue(registry.listClusters().contains("cluster1"));
    assertTrue(registry.listClusters().contains("cluster2"));
}

@Test
public void testClusterContextIsolation() {
    ClusterContext context1 = new ClusterContext("cluster1", config1);
    ClusterContext context2 = new ClusterContext("cluster2", config2);

    // Modify one context
    context1.executeProposals(proposals);

    // Other context should be unaffected
    assertEquals(ExecutorState.NO_TASK_IN_PROGRESS, context2.executorState());
}
```

### Integration Tests

```java
@Test
public void testMultiClusterRebalance() {
    // Setup: 3 test clusters
    KafkaCluster cluster1 = createTestCluster(3);
    KafkaCluster cluster2 = createTestCluster(5);
    KafkaCluster cluster3 = createTestCluster(4);

    // Register with Cruise Control
    MultiClusterCoordinator coordinator = new MultiClusterCoordinator();
    coordinator.registerCluster("c1", cluster1.config());
    coordinator.registerCluster("c2", cluster2.config());
    coordinator.registerCluster("c3", cluster3.config());

    // Execute batch rebalance
    coordinator.executeOnAll(context -> {
        context.rebalance(standardGoals);
        return null;
    });

    // Verify all clusters rebalanced
    assertTrue(coordinator.getContext("c1").isBalanced());
    assertTrue(coordinator.getContext("c2").isBalanced());
    assertTrue(coordinator.getContext("c3").isBalanced());
}
```

---

## Success Metrics

| Metric | Current (20 clusters) | Target | Improvement |
|--------|----------------------|--------|-------------|
| Deployment instances | 20 | 1 | 95% reduction |
| Total memory usage | 160GB | 21GB | 87% reduction |
| Configuration files | 20 | 1 + per-cluster | 70% reduction |
| Time to check all clusters | 10 min | 5 sec | 99% reduction |
| Ops FTEs required | 2-3 | 0.5 | 75-83% reduction |

---

## Effort Estimate

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: Core foundation | 4 weeks | 20 days |
| Phase 2: REST API | 2 weeks | 10 days |
| Phase 3: UI Dashboard | 4 weeks | 20 days |
| Phase 4: Advanced features | 4 weeks | 20 days |
| **Total** | **14 weeks** | **70 days** |

---

## Alternatives Considered

### Alternative 1: Separate Instances (Current State)

**Pros:** Simple, complete isolation

**Cons:** High overhead, no unified view, inefficient

**Decision:** Rejected - too costly at scale

### Alternative 2: External Orchestration Tool

**Example:** Separate dashboard tool that calls multiple Cruise Control instances

**Pros:** No changes to Cruise Control core

**Cons:**
- Still need N instances running
- No resource sharing
- API latency (multiple network hops)
- Complex failure modes

**Decision:** Rejected - doesn't solve resource problem

### Alternative 3: Kubernetes Operator

**Approach:** K8s operator that manages multiple Cruise Control pods

**Pros:** Leverages K8s for orchestration

**Cons:**
- Still N pods = N× resources
- K8s-only solution (not portable)
- Doesn't help non-K8s deployments

**Decision:** Complementary (can use operator to deploy multi-cluster CC)

---

## Future Enhancements

### Phase 2 Features

1. **Cross-Cluster Topic Migration**
   - Move topics between clusters
   - MirrorMaker integration
   - Schema registry coordination

2. **Cluster Cloning**
   - Copy configuration from one cluster to another
   - "Promote staging config to prod"

3. **Multi-Cluster Quotas**
   - Aggregate quota management
   - "No more than 3 clusters rebalancing simultaneously"

4. **Global Anomaly Detection**
   - Detect anomalies across cluster fleet
   - "US-West CPU 2× higher than US-East (investigate!)"

---

## References

- [LinkedIn's Multi-Datacenter Kafka](https://engineering.linkedin.com/kafka/running-kafka-scale)
- [Kubernetes Multi-Cluster Management](https://kubernetes.io/docs/concepts/cluster-administration/federation/)
- [Confluent Control Center](https://docs.confluent.io/platform/current/control-center/) - Commercial multi-cluster tool

---

**Priority:** P0 (Critical for enterprises with many clusters)
**Effort:** 50-70 dev-days
**Dependencies:** None (backward compatible)
**Breaking Changes:** None
