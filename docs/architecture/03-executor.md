# Executor: Safe Execution Engine

## Overview

The Executor is responsible for safely applying optimization proposals to the Kafka cluster. It implements a state machine that carefully orchestrates partition movements and leader elections while respecting throttling limits and monitoring progress.

## State Machine

![Execution State Machine](diagrams/execution-state-machine.mermaid)

### States

- **NO_TASK_IN_PROGRESS** - Idle state, ready to accept new execution
- **STARTING_EXECUTION** - Initializing execution tasks
- **INTER_BROKER_REPLICA_MOVEMENT** - Executing partition replica movements
- **LEADER_MOVEMENT** - Executing leadership elections
- **STOPPING_EXECUTION** - Gracefully stopping execution
- **STOPPED_WITH_ERROR** - Execution stopped due to error

### State Transitions

1. **NO_TASK_IN_PROGRESS → STARTING_EXECUTION**
   - Trigger: `executeProposals()` called
   - Precondition: No execution currently in progress
   - Action: Validate proposals and initialize ExecutionTasks

2. **STARTING_EXECUTION → INTER_BROKER_REPLICA_MOVEMENT**
   - Trigger: Tasks initialized successfully
   - Action: Begin submitting partition reassignments to Kafka

3. **INTER_BROKER_REPLICA_MOVEMENT → LEADER_MOVEMENT**
   - Trigger: All replica movements completed
   - Action: Begin leadership elections

4. **LEADER_MOVEMENT → NO_TASK_IN_PROGRESS**
   - Trigger: All leadership elections completed
   - Action: Clean up and mark execution complete

5. **[Any State] → STOPPING_EXECUTION**
   - Trigger: `stopExecution()` called
   - Action: Cancel pending tasks, wait for in-flight tasks

6. **STOPPING_EXECUTION → NO_TASK_IN_PROGRESS / STOPPED_WITH_ERROR**
   - Trigger: All tasks stopped
   - Action: Report final status

## Execution Phases

### Phase 1: Inter-Broker Replica Movement

**Goal:** Move partition replicas between brokers

**Algorithm:**

1. **Task Creation**
   ```
   For each ExecutionProposal:
     Create ExecutionTask with:
       - Partition (topic, partition number)
       - Source replica set
       - Target replica set
       - Priority (based on broker load, leadership)
   ```

2. **Task Sorting**
   - Sort by priority (high-load brokers first)
   - Group by topic to maintain ordering guarantees
   - Respect inter-partition dependencies

3. **Batch Submission**
   ```
   While tasks remain:
     Select next batch (size = maxConcurrentMovements)
     Submit via AdminClient.alterPartitionReassignments()
     Mark tasks as IN_PROGRESS
   ```

4. **Progress Monitoring**
   ```
   Every 10 seconds:
     For each IN_PROGRESS task:
       Query Kafka for current ISR
       If ISR matches target:
         Mark task as COMPLETED
         Submit next batch of tasks
   ```

**Configuration:**
- `num.concurrent.partition.movements.per.broker` (default: 5)
  - Controls how many partitions per broker can move simultaneously
  - Higher values = faster rebalance, more cluster impact

- `execution.progress.check.interval.ms` (default: 10000)
  - How often to check ISR status
  - Lower values = faster detection, more API calls

**Bandwidth Throttling:**

Cruise Control applies Kafka's built-in inter-broker throttle:

```java
// Set leader throttle
adminClient.incrementalAlterConfigs(Map.of(
  new ConfigResource(BROKER, brokerId),
  List.of(new AlterConfigOp(
    new ConfigEntry("leader.replication.throttled.rate", "10485760"),
    SET
  ))
));

// Set follower throttle
adminClient.incrementalAlterConfigs(Map.of(
  new ConfigResource(BROKER, brokerId),
  List.of(new AlterConfigOp(
    new ConfigEntry("follower.replication.throttled.rate", "10485760"),
    SET
  ))
));
```

Configuration:
- `replication.throttle` (default: 10 MB/s per broker)
- Throttle removed automatically after execution completes

### Phase 2: Leadership Movement

**Goal:** Transfer leadership to preferred replicas

**Algorithm:**

1. **Identify Leadership Changes**
   ```
   For each ExecutionProposal:
     If currentLeader != targetLeader:
       Add to leadershipMovements
   ```

2. **Execute Leader Elections**
   ```
   Batch leadershipMovements by broker
   For each batch:
     Call AdminClient.electLeaders(PREFERRED, partitions)
     Wait for completion
   ```

3. **Verify Leadership**
   ```
   For each partition:
     Query metadata to verify leader changed
     If leader != expected:
       Log warning and continue (non-fatal)
   ```

**Performance:**
- Leadership elections are fast (~100ms per partition)
- Can execute thousands per minute
- No data movement required

**Configuration:**
- `max.num.cluster.partition.movements` (default: 1250)
  - Total concurrent partition movements across cluster
  - Includes both replica movements and leadership

## Error Handling

### Recoverable Errors

1. **Partition Stuck in ISR Update**
   - **Detection:** Task in IN_PROGRESS for > `execution.timeout.ms`
   - **Action:**
     - Log warning
     - Continue with other tasks
     - Mark task as TIMED_OUT
   - **User Action:** Investigate broker/network issues

2. **AdminClient API Errors**
   - **Detection:** Exception from Kafka AdminClient
   - **Action:**
     - Retry with exponential backoff (up to 3 times)
     - If retry exhausted, mark task as FAILED
   - **User Action:** Check Kafka broker health

3. **Broker Failure During Execution**
   - **Detection:** Broker disconnects from ZooKeeper/Controller
   - **Action:**
     - Pause execution
     - Wait for broker recovery or timeout
     - Resume or abort based on configuration

### Non-Recoverable Errors

1. **Invalid Proposals**
   - **Detection:** Proposal references non-existent broker/partition
   - **Action:** Reject proposals before execution starts
   - **User Action:** Regenerate proposals with current cluster state

2. **Concurrent Execution**
   - **Detection:** Another execution already in progress
   - **Action:** Return HTTP 429 (Too Many Requests)
   - **User Action:** Wait for current execution to complete

3. **Kafka Cluster Unavailable**
   - **Detection:** Cannot connect to Kafka cluster
   - **Action:** Fail execution immediately
   - **User Action:** Check Kafka cluster availability

### Rollback Strategy

Cruise Control does NOT automatically rollback partition movements because:
1. Partition movements are one-way (data already moved)
2. Rollback would double the data movement cost
3. Partial progress is often acceptable

Instead, on error:
1. Stop accepting new tasks
2. Complete in-flight tasks
3. Report which tasks completed vs. failed
4. User can manually revert or continue

## Monitoring and Metrics

### JMX Metrics

```
kafka.cruisecontrol:type=Executor,name=state
- Current execution state

kafka.cruisecontrol:type=Executor,name=numFinishedPartitionMovements
- Total partition movements completed

kafka.cruisecontrol:type=Executor,name=numInProgressPartitionMovements
- Currently executing partition movements

kafka.cruisecontrol:type=Executor,name=numPendingPartitionMovements
- Queued partition movements

kafka.cruisecontrol:type=Executor,name=numAbortedPartitionMovements
- Failed/aborted partition movements
```

### Log Messages

**Info Level:**
```
[INFO] Starting execution with 150 partition movements and 50 leadership movements
[INFO] Completed 50/150 partition movements (33%)
[INFO] Execution completed successfully in 15m 32s
```

**Warning Level:**
```
[WARN] Partition [topic-0] stuck in ISR update for 5 minutes
[WARN] Failed to elect leader for [topic-1], will retry
```

**Error Level:**
```
[ERROR] Execution failed: Kafka cluster unavailable
[ERROR] Broker 3 failed during execution, aborting
```

## Configuration Reference

| Configuration | Default | Description |
|--------------|---------|-------------|
| `num.concurrent.partition.movements.per.broker` | 5 | Max concurrent movements per broker |
| `max.num.cluster.partition.movements` | 1250 | Max concurrent movements cluster-wide |
| `default.replication.throttle` | 10485760 | Replication throttle in bytes/sec (10 MB/s) |
| `execution.progress.check.interval.ms` | 10000 | ISR check interval (10 seconds) |
| `leader.movement.timeout.ms` | 180000 | Leadership election timeout (3 minutes) |
| `task.execution.alerting.threshold.ms` | 90000 | Alert if task takes longer (90 seconds) |

## Performance Tuning

### Faster Execution

To speed up rebalance execution:

1. **Increase concurrent movements:**
   ```
   num.concurrent.partition.movements.per.broker=10
   max.num.cluster.partition.movements=2500
   ```
   - Risk: Higher cluster load, potential instability

2. **Increase replication throttle:**
   ```
   default.replication.throttle=52428800  # 50 MB/s
   ```
   - Risk: Network saturation, impact on client traffic

3. **Decrease progress check interval:**
   ```
   execution.progress.check.interval.ms=5000  # 5 seconds
   ```
   - Risk: More API calls to Kafka, slight overhead

### Safer Execution

To minimize impact on production clusters:

1. **Decrease concurrent movements:**
   ```
   num.concurrent.partition.movements.per.broker=2
   max.num.cluster.partition.movements=500
   ```

2. **Decrease replication throttle:**
   ```
   default.replication.throttle=5242880  # 5 MB/s
   ```

3. **Execute during low-traffic periods**
   - Schedule via cron or manually trigger

## Example: Monitoring Execution

```bash
# Check execution state
curl -X GET "http://localhost:9090/kafkacruisecontrol/state?substates=executor"

# Response:
{
  "ExecutorState": {
    "state": "INTER_BROKER_REPLICA_MOVEMENT",
    "numFinishedPartitionMovements": 75,
    "numInProgressPartitionMovements": 5,
    "numPendingPartitionMovements": 70,
    "numCancelledPartitionMovements": 0,
    "numAbortedPartitionMovements": 0,
    "numDeadPartitionMovements": 0,
    "abortingPartitions": [],
    "abortedPartitions": [],
    "deadPartitions": [],
    "recentlyDemotedBrokers": [],
    "recentlyRemovedBrokers": []
  }
}
```

## Related Documentation

- [Architecture Overview](00-overview.md)
- [GoalOptimizer Internals](02-goal-optimizer.md)
- [Operations: Monitoring](../operations/monitoring.md)
