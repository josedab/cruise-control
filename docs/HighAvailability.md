# High Availability (HA) for Cruise Control

## Overview

Cruise Control now supports **active-passive high availability** through leader election, allowing multiple instances to run with automatic failover when the active leader fails. This addresses the single point of failure limitation in earlier versions.

## Architecture

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
```

### How It Works

1. **Leader Election**: Multiple Cruise Control instances compete for leadership using a coordination service (ZooKeeper, etcd, or Kubernetes)
2. **Active Leader**: One instance is elected as leader and actively manages the cluster
3. **Standby Instances**: Other instances monitor the leader's health and are ready to take over
4. **Automatic Failover**: When the leader fails, a standby instance is automatically promoted (typically within 10-30 seconds)

## Configuration

### Basic Configuration

Add these properties to your `cruisecontrol.properties`:

```properties
# Enable HA mode
cruise.control.ha.enabled=true

# Coordination service (zookeeper, etcd, or kubernetes)
cruise.control.ha.coordination.service=zookeeper

# Unique ID for this instance (defaults to hostname if not set)
cruise.control.ha.instance.id=cruise-control-1
```

### ZooKeeper Configuration

When using ZooKeeper as the coordination service:

```properties
# ZooKeeper connection string
cruise.control.ha.zookeeper.connect=zk1:2181,zk2:2181,zk3:2181

# ZooKeeper path for leader election
cruise.control.ha.zookeeper.election.path=/cruise-control/leader

# Session timeout (leader loses leadership if it doesn't heartbeat within this time)
cruise.control.ha.zookeeper.session.timeout.ms=30000
```

### State Synchronization Configuration

```properties
# Kafka topic for execution state checkpoints
cruise.control.ha.execution.state.topic=__CruiseControlExecutionState

# How often to checkpoint execution state
cruise.control.ha.execution.state.checkpoint.interval.ms=10000

# Maximum time to wait for state recovery during failover
cruise.control.ha.state.recovery.timeout.ms=60000
```

### Complete Configuration Example

```properties
# ============================================
# High Availability Configuration
# ============================================

# Enable HA
cruise.control.ha.enabled=true

# Use ZooKeeper for coordination
cruise.control.ha.coordination.service=zookeeper
cruise.control.ha.zookeeper.connect=zk1:2181,zk2:2181,zk3:2181
cruise.control.ha.zookeeper.election.path=/cruise-control/leader
cruise.control.ha.zookeeper.session.timeout.ms=30000

# Instance identification
cruise.control.ha.instance.id=cruise-control-1

# State management
cruise.control.ha.execution.state.topic=__CruiseControlExecutionState
cruise.control.ha.execution.state.checkpoint.interval.ms=10000
cruise.control.ha.state.recovery.timeout.ms=60000
cruise.control.ha.failover.timeout.ms=10000
```

## Deployment

### Kubernetes Deployment

Example deployment with 2 replicas for HA:

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
          value: "zookeeper"
        - name: CRUISE_CONTROL_HA_ZOOKEEPER_CONNECT
          value: "zk-service:2181"
        - name: CRUISE_CONTROL_HA_INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 9090
        livenessProbe:
          httpGet:
            path: /kafkacruisecontrol/state
            port: 9090
          periodSeconds: 10
```

### Docker Compose Deployment

```yaml
version: '3.8'
services:
  cruise-control-1:
    image: cruise-control:latest
    environment:
      - CRUISE_CONTROL_HA_ENABLED=true
      - CRUISE_CONTROL_HA_INSTANCE_ID=cruise-control-1
      - CRUISE_CONTROL_HA_ZOOKEEPER_CONNECT=zookeeper:2181
    ports:
      - "9090:9090"

  cruise-control-2:
    image: cruise-control:latest
    environment:
      - CRUISE_CONTROL_HA_ENABLED=true
      - CRUISE_CONTROL_HA_INSTANCE_ID=cruise-control-2
      - CRUISE_CONTROL_HA_ZOOKEEPER_CONNECT=zookeeper:2181
    ports:
      - "9091:9090"

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
```

## Monitoring

### JMX Metrics

The HA implementation exposes the following JMX metrics:

- `kafka.cruisecontrol:type=HAManager,name=IsLeader`
  - Gauge: 1 if leader, 0 if standby
- `kafka.cruisecontrol:type=HAManager,name=LeadershipChanges`
  - Counter: Number of leadership transitions
- `kafka.cruisecontrol:type=HAManager,name=FailoverDuration`
  - Histogram: Time taken for failover (milliseconds)
- `kafka.cruisecontrol:type=HAManager,name=CoordinationServiceConnected`
  - Gauge: 1 if connected, 0 if disconnected

### Logs

Monitor logs for HA events:

```
INFO  [main] HAManager - HA manager initialized successfully
INFO  [main] HAManager - Starting HA manager
INFO  [LeaderSelector-0] ZooKeeperLeaderElector - Instance cruise-control-1 acquired leadership
INFO  [main] KafkaCruiseControl - Transitioning to LEADER mode
INFO  [main] KafkaCruiseControl - Transitioned to LEADER mode successfully in 234ms
```

## Failover Behavior

### Expected Failover Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| Detection | 3-8s | ZooKeeper detects leader failure (session timeout) |
| Election | 1-3s | New leader elected from standbys |
| State Recovery | 5-15s | New leader recovers execution state |
| **Total** | **10-25s** | Complete failover time |

### What Happens During Failover

1. **Leader Failure**: Active instance crashes or loses network connectivity
2. **Detection**: ZooKeeper detects missing heartbeats within session timeout
3. **Election**: Standby instances compete for leadership; one is elected
4. **Transition**: New leader:
   - Recovers execution state from Kafka topics
   - Enables anomaly detection
   - Resumes any in-flight operations
5. **Active**: New leader is fully operational

### Handling Split-Brain

The implementation prevents split-brain scenarios through:
- **Fencing tokens**: Each leader has an epoch number
- **ZooKeeper guarantees**: Only one leader can hold the lock at a time
- **Idempotent operations**: Duplicate operations are safely ignored

## Operations

### Check Current Leader

You can determine which instance is the leader through:

1. **JMX Metric**: Check `IsLeader` gauge on each instance
2. **Logs**: Look for "acquired leadership" messages
3. **API** (future): `GET /kafkacruisecontrol/ha/status`

### Graceful Leadership Transfer

To transfer leadership (for maintenance):

```java
// Via API (future implementation)
POST /kafkacruisecontrol/ha/resign

// Via JMX
invoke kafka.cruisecontrol:type=HAManager resignLeadership()
```

### Rolling Upgrades

1. Upgrade standby instance first
2. Transfer leadership to upgraded instance
3. Upgrade original leader (now standby)
4. Both instances now running new version

## Troubleshooting

### Issue: No Leader Elected

**Symptoms**: Both instances show `IsLeader=0`

**Possible Causes**:
- ZooKeeper connection issues
- Network partitions
- Configuration mismatch

**Solution**:
1. Check ZooKeeper connectivity: `zkCli.sh -server zk1:2181`
2. Verify ZooKeeper quorum is healthy
3. Check `cruise.control.ha.zookeeper.connect` on all instances

### Issue: Frequent Leadership Changes

**Symptoms**: `LeadershipChanges` counter incrementing rapidly

**Possible Causes**:
- Network instability
- ZooKeeper session timeout too low
- Leader instance under high load

**Solution**:
1. Increase `cruise.control.ha.zookeeper.session.timeout.ms`
2. Check network latency between instances and ZooKeeper
3. Monitor leader instance resource usage

### Issue: Slow Failover

**Symptoms**: Failover takes >60 seconds

**Possible Causes**:
- State recovery timeout
- Large execution state
- Network issues

**Solution**:
1. Increase `cruise.control.ha.state.recovery.timeout.ms`
2. Reduce `cruise.control.ha.execution.state.checkpoint.interval.ms`
3. Check Kafka cluster health

## Limitations

1. **Active-Passive Only**: Only one instance is active at a time (not active-active)
2. **Failover Window**: Brief unavailability (10-30s) during failover
3. **State Recovery**: Long-running operations may need to restart after failover
4. **Resource Overhead**: Standby instances consume ~50-75% of active instance resources

## Future Enhancements

Planned improvements for future releases:
- Kubernetes-native leader election (using Leases)
- etcd coordination service support
- Hot standby with zero-downtime failover
- API endpoints for HA status and control
- Dashboard showing cluster topology

## Related Documentation

- [RFC-0006: Leader Election HA](../analysis-output/rfcs/RFC-0006-leader-election-ha.md)
- [Configuration Guide](./config.md)
- [Operations Guide](./operations.md)
