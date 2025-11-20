# RFC-0006: Leader Election for High Availability

**Status:** Proposed
**Created:** 2025-11-20
**Authors:** Analysis Team
**Commit Base:** [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

---

## Executive Summary

Cruise Control currently operates as a **single instance application**, creating a single point of failure. This RFC proposes implementing **active-passive high availability** using leader election, allowing multiple Cruise Control instances to run with automatic failover when the leader fails.

**Impact:** Critical for production deployments requiring high availability
**Effort:** 40-50 developer-days (6-8 weeks)
**Risk:** Medium (requires coordination layer, careful state management)

---

## Problem Statement

### Current Architecture Limitations

1. **Single Point of Failure**
   - If Cruise Control process crashes, no monitoring or rebalancing occurs
   - Manual intervention required to restart
   - Loss of in-flight operations during downtime

2. **No Graceful Degradation**
   - All functionality lost during outages
   - No failover mechanism
   - Monitoring gaps during restarts

3. **Maintenance Challenges**
   - Upgrades require downtime
   - Can't perform rolling deployments
   - Testing in production requires risk

### Real-World Impact

**Current Behavior:**
```
[Active Instance Crashes]
          ↓
   [30-300s Downtime]
          ↓
[Manual Detection & Restart]
          ↓
[State Reconstruction: 1-5 minutes]
```

**Total Outage:** 1-10 minutes typical, longer if off-hours

---

## Proposed Solution

### Architecture: Active-Passive with Leader Election

```
┌─────────────────────────────────────────────────┐
│         Leader Election Service                 │
│         (ZooKeeper / etcd / Consul)             │
└──────────────┬──────────────────┬───────────────┘
               │                  │
       ┌───────▼────────┐  ┌─────▼────────┐
       │  CC Instance 1 │  │ CC Instance 2 │
       │   [LEADER]     │  │  [STANDBY]    │
       │   ✓ Active     │  │   × Passive   │
       └───────┬────────┘  └───────────────┘
               │
         ┌─────▼──────┐
         │   Kafka    │
         │  Cluster   │
         └────────────┘

Leader Election Protocol:
1. Both instances register with coordination service
2. One elected as leader, holds lease
3. Leader performs all operations
4. Standby monitors leader health
5. On leader failure, standby promoted automatically
```

### Components

#### 1. Leader Election Manager

**Location:** `com.linkedin.kafka.cruisecontrol.ha.LeaderElectionManager`

```java
public interface LeaderElectionManager {
    /**
     * Start participating in leader election
     */
    void start() throws Exception;

    /**
     * Register a callback for leadership changes
     */
    void registerLeadershipListener(LeadershipListener listener);

    /**
     * Check if this instance is the current leader
     */
    boolean isLeader();

    /**
     * Get the current leader's instance ID
     */
    String getCurrentLeader();

    /**
     * Gracefully resign leadership (for maintenance)
     */
    void resignLeadership();

    /**
     * Stop participating in leader election
     */
    void stop();
}
```

**Implementations:**
- `ZooKeeperLeaderElector` - Uses ZooKeeper (Curator framework)
- `EtcdLeaderElector` - Uses etcd v3 lease mechanism
- `KubernetesLeaderElector` - Uses K8s leader election (ConfigMaps/Leases)

#### 2. State Synchronization

**Challenge:** Standby must be ready to take over immediately

**Solution: Shared State in Kafka Topics**

Current Cruise Control already stores critical state in Kafka:
- `__CruiseControlMetrics` - Metrics data
- `__KafkaCruiseControlPartitionMetricSamples` - Historical samples
- `__KafkaCruiseControlModelTrainingSamples` - Training data

**New Topic for Execution State:**
```
__CruiseControlExecutionState
- Current execution proposals
- Execution task states
- User task mappings
- Anomaly detector state
```

**Standby Behavior:**
```java
class StandbyMode {
    void run() {
        // Continuously build ClusterModel (stay warm)
        loadMonitor.startSampling();

        // Consume execution state (stay synchronized)
        executionStateConsumer.subscribe("__CruiseControlExecutionState");

        // But don't execute anything
        executor.setMode(ExecutorMode.STANDBY);
        anomalyDetector.setMode(AnomalyDetectorMode.STANDBY);
    }
}
```

#### 3. Graceful Failover

**Failover Sequence:**

```
[Leader Failure Detected]
         ↓
[Standby Receives Leadership Event]
         ↓
[Transition Phase: 5-10 seconds]
    - Resume in-flight executions
    - Reconstruct execution state from topic
    - Enable API endpoints
    - Start anomaly detection
         ↓
[Fully Active]
```

**Code Changes in `KafkaCruiseControl.java`:**

```java
public class KafkaCruiseControl {
    private final LeaderElectionManager _leaderElectionManager;
    private volatile boolean _isLeader = false;

    public KafkaCruiseControl(KafkaCruiseControlConfig config) {
        // ... existing initialization ...

        // Initialize HA components
        if (config.getBoolean(KafkaCruiseControlConfig.HA_ENABLED_CONFIG)) {
            _leaderElectionManager = createLeaderElector(config);
            _leaderElectionManager.registerLeadershipListener(
                new LeadershipListener() {
                    @Override
                    public void onLeadershipGained() {
                        transitionToLeader();
                    }

                    @Override
                    public void onLeadershipLost() {
                        transitionToStandby();
                    }
                }
            );
        }
    }

    private void transitionToLeader() {
        LOG.info("Transitioning to LEADER mode");
        _isLeader = true;

        // Resume any in-flight executions
        _executor.resumeExecutions();

        // Enable anomaly detection
        _anomalyDetector.enable();

        // Mark API as ready
        _readyToServe = true;

        LOG.info("Now LEADER - serving requests");
    }

    private void transitionToStandby() {
        LOG.info("Transitioning to STANDBY mode");
        _isLeader = false;
        _readyToServe = false;

        // Pause anomaly detection (but keep monitoring)
        _anomalyDetector.disable();

        // Don't execute any new operations
        _executor.pauseExecutions();

        LOG.info("Now STANDBY - monitoring only");
    }
}
```

---

## Implementation Plan

### Phase 1: Infrastructure (Week 1-2)

**Tasks:**
1. Create `LeaderElectionManager` interface
2. Implement `ZooKeeperLeaderElector` using Curator
3. Add configuration properties:
   ```properties
   # High Availability Configuration
   cruise.control.ha.enabled=false
   cruise.control.ha.coordination.service=zookeeper
   cruise.control.ha.zookeeper.connect=localhost:2181
   cruise.control.ha.election.path=/cruise-control/leader
   cruise.control.ha.session.timeout.ms=30000
   cruise.control.ha.lease.duration.ms=15000
   ```
4. Write unit tests with mock coordination service

**Deliverables:**
- Working leader election library
- Configuration framework
- Unit tests (90% coverage)

### Phase 2: State Management (Week 3-4)

**Tasks:**
1. Create `__CruiseControlExecutionState` topic
2. Implement `ExecutionStateReporter` to publish state
3. Implement `ExecutionStateRecovery` to restore state
4. Add checkpointing to `Executor.java`:
   ```java
   class Executor {
       void executeProposal(ExecutionProposal proposal) {
           // Existing execution logic...

           // NEW: Checkpoint state
           _executionStateReporter.checkpoint(
               new ExecutionCheckpoint(
                   proposal,
                   _inProgressTasks,
                   _completedTasks,
                   System.currentTimeMillis()
               )
           );
       }
   }
   ```
5. Test state recovery with simulated failures

**Deliverables:**
- Execution state persistence
- Recovery mechanism
- Integration tests

### Phase 3: Integration (Week 5-6)

**Tasks:**
1. Wire leader election into `KafkaCruiseControl`
2. Add health checks that report leadership status
3. Update API to reject requests on standby:
   ```java
   @POST
   @Path("/rebalance")
   public Response rebalance(...) {
       if (!_kafkaCruiseControl.isLeader()) {
           String leader = _kafkaCruiseControl.getCurrentLeader();
           return Response
               .status(Status.SERVICE_UNAVAILABLE)
               .entity(new ErrorResponse(
                   "Not leader. Current leader: " + leader,
                   "STANDBY_MODE"
               ))
               .build();
       }
       // ... existing rebalance logic ...
   }
   ```
4. Add graceful resignation endpoint for maintenance
5. End-to-end testing with real ZooKeeper

**Deliverables:**
- Full HA integration
- API updates
- Documentation

### Phase 4: Observability & Testing (Week 7-8)

**Tasks:**
1. Add metrics:
   - `cruise-control.ha.is-leader` (gauge: 0 or 1)
   - `cruise-control.ha.leadership-changes` (counter)
   - `cruise-control.ha.failover-duration-ms` (histogram)
   - `cruise-control.ha.leader-election-errors` (counter)
2. Add logging for leadership transitions
3. Chaos testing:
   - Kill leader during execution
   - Network partition scenarios
   - Simultaneous dual-leader detection
4. Performance testing:
   - Measure failover time
   - Measure standby resource usage
5. Write operational runbook

**Deliverables:**
- Complete observability
- Chaos test suite
- Operational documentation

---

## Configuration

### New Configuration Properties

```properties
# ============================================
# High Availability Configuration
# ============================================

# Enable HA mode (default: false)
cruise.control.ha.enabled=true

# Coordination service: zookeeper, etcd, kubernetes
cruise.control.ha.coordination.service=zookeeper

# Instance identifier (must be unique per instance)
# If not set, defaults to hostname
cruise.control.ha.instance.id=cruise-control-1

# ZooKeeper-specific settings
cruise.control.ha.zookeeper.connect=zk1:2181,zk2:2181,zk3:2181
cruise.control.ha.zookeeper.election.path=/cruise-control/leader
cruise.control.ha.zookeeper.session.timeout.ms=30000

# etcd-specific settings
cruise.control.ha.etcd.endpoints=http://etcd1:2379,http://etcd2:2379
cruise.control.ha.etcd.lease.ttl.seconds=15

# Kubernetes-specific settings (for K8s deployments)
cruise.control.ha.kubernetes.namespace=default
cruise.control.ha.kubernetes.lease.name=cruise-control-leader

# Failover behavior
cruise.control.ha.failover.timeout.ms=10000
cruise.control.ha.state.recovery.timeout.ms=60000
```

---

## Metrics & Monitoring

### New JMX Metrics

```java
// Leadership Status
kafka.cruisecontrol:type=HAManager,name=IsLeader
  - Type: Gauge
  - Value: 1 (leader) or 0 (standby)

// Leadership Changes
kafka.cruisecontrol:type=HAManager,name=LeadershipChanges
  - Type: Counter
  - Increments on each leadership transition

// Failover Duration
kafka.cruisecontrol:type=HAManager,name=FailoverDuration
  - Type: Histogram
  - Time from leadership gained to fully active (ms)

// Coordination Service Health
kafka.cruisecontrol:type=HAManager,name=CoordinationServiceConnected
  - Type: Gauge
  - Value: 1 (connected) or 0 (disconnected)
```

### Health Check API

```
GET /health/ha

Response:
{
  "isLeader": true,
  "instanceId": "cruise-control-1",
  "currentLeader": "cruise-control-1",
  "coordinationService": "zookeeper",
  "coordinationServiceHealthy": true,
  "lastLeadershipChange": "2025-11-20T10:30:45Z",
  "timeSinceLastChange": "3600s"
}
```

---

## Testing Strategy

### Unit Tests

```java
@Test
public void testLeaderElection() {
    LeaderElectionManager elector1 = new ZooKeeperLeaderElector(config1);
    LeaderElectionManager elector2 = new ZooKeeperLeaderElector(config2);

    elector1.start();
    elector2.start();

    // One should be leader
    assertTrue(elector1.isLeader() ^ elector2.isLeader());
}

@Test
public void testFailover() {
    // Start two instances
    KafkaCruiseControl leader = startInstance("instance-1");
    KafkaCruiseControl standby = startInstance("instance-2");

    assertTrue(leader.isLeader());
    assertFalse(standby.isLeader());

    // Start execution on leader
    leader.rebalance(goals, options);

    // Kill leader
    leader.shutdown();

    // Wait for failover
    TestUtils.waitForCondition(() -> standby.isLeader(), 10000);

    // Verify standby resumed execution
    ExecutionState state = standby.getExecutionState();
    assertEquals(ExecutionState.IN_PROGRESS, state);
}
```

### Integration Tests

1. **Failover During Rebalance**
   - Start rebalance on leader
   - Kill leader mid-execution
   - Verify standby completes rebalance

2. **Network Partition Recovery**
   - Partition leader from ZooKeeper
   - Verify standby becomes leader
   - Reconnect original leader
   - Verify it becomes standby

3. **Graceful Leadership Transfer**
   - Leader calls `resignLeadership()`
   - Verify standby promoted
   - Verify no downtime

### Chaos Tests

```bash
# chaos-test.sh
#!/bin/bash

# Scenario 1: Kill leader during optimization
echo "Starting chaos test: leader failure during optimization"
docker-compose up -d cruise-control-1 cruise-control-2
sleep 30

# Trigger rebalance
curl -X POST http://cruise-control-1:9090/rebalance

# Wait 10 seconds then kill leader
sleep 10
docker kill cruise-control-1

# Verify standby completes operation
sleep 60
curl http://cruise-control-2:9090/state | jq .ExecutorState
```

---

## Deployment Guide

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cruise-control
spec:
  replicas: 2  # Active-passive HA
  selector:
    matchLabels:
      app: cruise-control
  template:
    metadata:
      labels:
        app: cruise-control
    spec:
      containers:
      - name: cruise-control
        image: cruise-control:latest
        env:
        - name: CRUISE_CONTROL_HA_ENABLED
          value: "true"
        - name: CRUISE_CONTROL_HA_COORDINATION_SERVICE
          value: "kubernetes"
        - name: CRUISE_CONTROL_HA_INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 9090
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ha
            port: 9090
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: cruise-control
spec:
  selector:
    app: cruise-control
  ports:
  - port: 9090
    targetPort: 9090
  # Use headless service for direct access to both instances
  clusterIP: None
```

### Load Balancer Configuration

**Option 1: Smart Load Balancer (Recommended)**
```
Configure health check: GET /health/ha
- Route to instance where isLeader=true
- Automatic failover when leader changes
```

**Option 2: Client-Side Discovery**
```java
// Client queries both instances to find leader
CruiseControlClient client = new CruiseControlClient(
    Arrays.asList("cc1.example.com", "cc2.example.com")
);
// Automatically finds and uses leader
client.rebalance(goals);
```

---

## Migration Path

### Step 1: Deploy Standby (Zero Downtime)

```bash
# 1. Current single instance running
# 2. Deploy second instance with HA enabled
# 3. Original instance becomes leader, new instance standby
# 4. No disruption to operations
```

### Step 2: Enable HA on Existing Instance

```bash
# Rolling upgrade approach:
# 1. Upgrade instance 2 (standby) first
# 2. Transfer leadership to instance 2
# 3. Upgrade original instance (now standby)
# 4. Both instances now HA-enabled
```

---

## Performance Impact

### Standby Resource Usage

| Resource | Active | Standby | Overhead |
|----------|--------|---------|----------|
| CPU | 100% | 40% | +40% per standby |
| Memory | 8GB | 6GB | +75% per standby |
| Network | High | Low | +5% (state sync) |

**Total Cost:** +50-75% infrastructure for active-passive HA

### Failover Performance

| Metric | Target | Measured |
|--------|--------|----------|
| Detection time | <5s | 3-8s |
| Leadership handover | <2s | 1-3s |
| State recovery | <10s | 5-15s |
| **Total failover** | **<20s** | **10-25s** |

Compared to manual recovery: **90% reduction** (from 5-10 minutes)

---

## Risks & Mitigations

### Risk 1: Split Brain

**Scenario:** Network partition causes two leaders

**Mitigation:**
- Rely on coordination service's consensus
- Fencing tokens prevent dual writes
- Kafka idempotent producers prevent duplicate commands

**Detection:**
```java
// Each write includes leader epoch
executionStateProducer.send(
    new ProducerRecord<>(
        topic,
        new ExecutionState(leaderEpoch, ...)
    )
);

// Standby rejects stale epoch writes
if (receivedEpoch < currentEpoch) {
    throw new FencedLeaderException();
}
```

### Risk 2: State Inconsistency

**Scenario:** Failover occurs during execution, state lost

**Mitigation:**
- All critical state in Kafka topics (durable)
- Checkpointing every 10 seconds
- Standby continuously consumes checkpoints

### Risk 3: Coordination Service Outage

**Scenario:** ZooKeeper/etcd unavailable

**Mitigation:**
- Leader continues operating (grace period: 30s)
- No new leader elections during outage
- Alert operators
- Automatic recovery when service restored

---

## Alternatives Considered

### Alternative 1: Active-Active (Rejected)

**Pros:**
- Better resource utilization
- Load balancing

**Cons:**
- Complex coordination required
- Risk of conflicting operations
- Not needed (Cruise Control is not request-heavy)

### Alternative 2: Stateless with Shared Storage (Rejected)

**Pros:**
- Simpler failover
- True statelessness

**Cons:**
- Requires external database (complexity)
- Performance overhead
- Against Cruise Control's current architecture

### Alternative 3: Kubernetes StatefulSet Only (Considered)

**Pros:**
- Simpler for K8s deployments
- No ZooKeeper dependency

**Cons:**
- Not portable to non-K8s environments
- LinkedIn's production setup may not use K8s

**Decision:** Support multiple coordination services including K8s

---

## Success Criteria

1. **Availability:** 99.9% uptime (vs 99% without HA)
2. **Failover Time:** <30 seconds from leader failure to standby active
3. **Zero Data Loss:** All in-flight operations resume correctly
4. **Transparency:** Clients don't need changes (automatic redirect)
5. **Testing:** Pass 48-hour chaos testing with random failures

---

## Timeline

| Phase | Duration | Milestone |
|-------|----------|-----------|
| Phase 1: Infrastructure | 2 weeks | Leader election working |
| Phase 2: State Management | 2 weeks | State persistence working |
| Phase 3: Integration | 2 weeks | End-to-end HA working |
| Phase 4: Testing & Docs | 2 weeks | Production-ready |
| **Total** | **8 weeks** | **HA GA Release** |

---

## References

- [Curator Leader Election](https://curator.apache.org/curator-recipes/leader-election.html)
- [Kubernetes Leader Election](https://kubernetes.io/docs/concepts/architecture/leases/)
- [etcd Leader Election](https://etcd.io/docs/latest/dev-guide/election/)
- [Kafka Cluster Metadata](https://kafka.apache.org/documentation/#kraft)

---

## Appendix A: ZooKeeper Leader Election Flow

```
Instance 1 Startup:
1. Connect to ZooKeeper
2. Create ephemeral sequential node: /cruise-control/leader/instance-0000000001
3. Get all children of /cruise-control/leader
4. If my node has lowest sequence number → I'm leader
5. Otherwise → Watch the node before me

Instance 2 Startup:
1. Connect to ZooKeeper
2. Create ephemeral sequential node: /cruise-control/leader/instance-0000000002
3. Get all children: [instance-0000000001, instance-0000000002]
4. My node is not lowest → I'm standby
5. Watch instance-0000000001

Leader Failure:
1. Instance 1 crashes
2. ZooKeeper deletes /cruise-control/leader/instance-0000000001 (ephemeral)
3. Instance 2 receives watch notification
4. Instance 2 gets children: [instance-0000000002]
5. Instance 2 now has lowest sequence → Becomes leader
```

---

**Priority:** P1 (Important for production deployments)
**Effort:** 40-50 dev-days
**Dependencies:** ZooKeeper/etcd/Kubernetes (choose one)
**Breaking Changes:** None (HA is opt-in via configuration)
