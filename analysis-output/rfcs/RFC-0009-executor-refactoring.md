# RFC-0009: Executor Refactoring - Decomposing the God Class

**Status:** Proposed
**Created:** 2025-11-20
**Authors:** Analysis Team
**Commit Base:** [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

---

## Executive Summary

`Executor.java` is a **"god class"** with **~3,500 lines of code** handling multiple responsibilities: execution orchestration, state management, task scheduling, throttling, monitoring, and Kafka API interaction. This RFC proposes refactoring it into **7 focused, single-responsibility classes** to improve maintainability, testability, and future extensibility.

**Impact:** Improved code maintainability, easier testing, enables future enhancements
**Effort:** 30-40 developer-days (6-8 weeks)
**Risk:** Medium (requires careful refactoring with comprehensive testing)

---

## Problem Statement

### Current State: The Executor God Class

**File:** `cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/executor/Executor.java`

**Statistics:**
- **Lines of Code:** ~3,500
- **Methods:** 80+
- **Cyclomatic Complexity:** High (15-20 per complex method)
- **Responsibilities:** 7+ distinct concerns

### Identified Responsibilities

| Responsibility | LOC | Current Location | Issue |
|----------------|-----|------------------|-------|
| **Execution Orchestration** | ~500 | `executeProposals()`, `stopExecution()` | Mixed with state management |
| **Task Management** | ~800 | `addExecutionTasks()`, `processTask()` | Complex task lifecycle logic |
| **State Machine** | ~400 | `maybeUpdateOngoingExecutionState()` | Hidden in large method |
| **Throttling** | ~300 | `throttling()`, `setThrottles()` | Spread across multiple methods |
| **Kafka API Interaction** | ~600 | `submitReplicaReassignmentTasks()`, `electLeaders()` | Mixed with logic |
| **Monitoring & Metrics** | ~400 | `updateMetrics()`, `logProgress()` | Scattered throughout |
| **Configuration** | ~200 | Various getters/setters | No clear ownership |
| **Utilities** | ~300 | Helper methods | Should be in separate utility class |

### Maintenance Challenges

#### 1. **Difficult to Test**

```java
// Current: Can't test throttling logic without full Executor
@Test
public void testThrottling() {
    // Must create entire Executor with all dependencies
    Executor executor = new Executor(config, time, metrics,
        metricRegistry, kafkaAdminClient, ...); // 10+ dependencies!

    // Can't mock internal state
    // Can't test throttling in isolation
}
```

#### 2. **Hard to Understand**

**Example: `maybeUpdateOngoingExecutionState()` method is 200+ lines:**
- Queries Kafka
- Updates task states
- Handles errors
- Updates metrics
- Logs progress
- Checks for stuck tasks

**New engineers report:** "It takes 2-3 weeks to understand Executor.java"

#### 3. **Fragile Changes**

- Changing throttling logic risks breaking state machine
- Adding new task types requires modifying multiple methods
- Performance improvements touch many unrelated areas

#### 4. **Low Cohesion**

Methods like `executeProposals()` call 15+ other methods spanning different concerns, making it impossible to reason about behavior locally.

---

## Proposed Solution

### Refactored Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Executor                              │
│               (Facade/Coordinator)                       │
│                  ~300 LOC                                │
└────┬────────────┬────────────┬──────────┬───────────────┘
     │            │            │          │
     ▼            ▼            ▼          ▼
┌─────────┐ ┌──────────┐ ┌─────────┐ ┌──────────┐
│Execution│ │  Task    │ │ State   │ │Throttle  │
│Planner  │ │ Manager  │ │ Machine │ │Manager   │
│~400 LOC │ │ ~600 LOC │ │~500 LOC │ │~400 LOC  │
└────┬────┘ └────┬─────┘ └────┬────┘ └────┬─────┘
     │           │            │           │
     │           └────────────┴───────────┘
     │                    │
     ▼                    ▼
┌──────────────┐   ┌────────────────┐
│KafkaExecutor │   │ExecutionMetrics│
│~500 LOC      │   │~300 LOC        │
└──────────────┘   └────────────────┘
     │
     ▼
┌──────────────────┐
│ExecutionTaskRepo │
│~200 LOC          │
└──────────────────┘
```

### New Classes

#### 1. **ExecutionPlanner**

**Responsibility:** Plan execution phases and ordering

**Location:** `com.linkedin.kafka.cruisecontrol.executor.plan.ExecutionPlanner`

```java
/**
 * Plans the execution of optimization proposals by organizing them into phases.
 *
 * <p>Execution proceeds in two phases:
 * <ol>
 *   <li><b>Phase 1:</b> Inter-broker replica movements</li>
 *   <li><b>Phase 2:</b> Leadership transfers</li>
 * </ol>
 */
public class ExecutionPlanner {
    /**
     * Creates an execution plan from optimizer proposals.
     *
     * @param proposals the optimization proposals to execute
     * @param options execution options (e.g., concurrent movements limit)
     * @return ordered execution plan with phases
     */
    public ExecutionPlan plan(Collection<ExecutionProposal> proposals,
                             ExecutionOptions options) {
        // Phase 1: Organize replica movements
        List<ReplicaMovementTask> replicaMovements =
            createReplicaMovementTasks(proposals);

        // Phase 2: Organize leadership changes
        List<LeadershipMovementTask> leadershipChanges =
            createLeadershipTasks(proposals);

        // Prioritize tasks (dead brokers first, etc.)
        prioritizeTasks(replicaMovements, options);

        return new ExecutionPlan(replicaMovements, leadershipChanges);
    }

    private void prioritizeTasks(List<ReplicaMovementTask> tasks,
                                ExecutionOptions options) {
        // Priority order:
        // 1. Moves off dead/decommissioned brokers (highest)
        // 2. Moves for goal violations
        // 3. Optimization moves (lowest)
    }
}
```

**Key Methods:**
- `plan(proposals, options)` - Create execution plan
- `createReplicaMovementTasks()` - Convert proposals to tasks
- `prioritizeTasks()` - Order tasks by priority

**Benefits:**
- Clear separation of planning from execution
- Easy to test different prioritization strategies
- ~400 LOC, focused responsibility

---

#### 2. **ExecutionTaskManager**

**Responsibility:** Manage task lifecycle and state transitions

**Location:** `com.linkedin.kafka.cruisecontrol.executor.task.ExecutionTaskManager`

```java
/**
 * Manages the lifecycle of execution tasks.
 *
 * <p>Tasks transition through states:
 * <pre>
 *   PENDING → IN_PROGRESS → COMPLETED
 *                  ↓
 *               ABORTING → ABORTED
 * </pre>
 */
public class ExecutionTaskManager {
    private final ExecutionTaskRepository _taskRepository;
    private final ExecutionStateMachine _stateMachine;

    /**
     * Submits new tasks for execution.
     *
     * @param tasks the tasks to execute
     */
    public void submitTasks(List<ExecutionTask> tasks) {
        for (ExecutionTask task : tasks) {
            _taskRepository.save(task);
            _stateMachine.transition(task, ExecutionState.PENDING);
        }
    }

    /**
     * Updates task state based on cluster observations.
     *
     * @param taskId the task to update
     * @param observation current cluster state observation
     * @return true if task transitioned to new state
     */
    public boolean updateTaskState(String taskId,
                                  ClusterStateObservation observation) {
        ExecutionTask task = _taskRepository.get(taskId);

        // Determine if state transition should occur
        ExecutionState newState = determineNewState(task, observation);

        if (newState != task.state()) {
            _stateMachine.transition(task, newState);
            return true;
        }
        return false;
    }

    /**
     * Gets all tasks in a given state.
     */
    public List<ExecutionTask> getTasksInState(ExecutionState state) {
        return _taskRepository.findByState(state);
    }
}
```

**Key Methods:**
- `submitTasks()` - Add new tasks
- `updateTaskState()` - Transition task states
- `getTasksInState()` - Query tasks
- `getTaskProgress()` - Calculate completion percentage

**Benefits:**
- Single source of truth for task state
- Easy to test state transitions
- Cleaner task queries
- ~600 LOC

---

#### 3. **ExecutionStateMachine**

**Responsibility:** Enforce state transition rules

**Location:** `com.linkedin.kafka.cruisecontrol.executor.state.ExecutionStateMachine`

```java
/**
 * Enforces valid state transitions for execution.
 *
 * <p>Valid transitions:
 * <pre>
 *   NO_TASK_IN_PROGRESS → STARTING_EXECUTION
 *   STARTING_EXECUTION → INTER_BROKER_REPLICA_MOVEMENT
 *   INTER_BROKER_REPLICA_MOVEMENT → LEADER_MOVEMENT
 *   LEADER_MOVEMENT → NO_TASK_IN_PROGRESS
 *   * → STOPPING_EXECUTION (any state can be stopped)
 * </pre>
 */
public class ExecutionStateMachine {
    private volatile ExecutionState _currentState = NO_TASK_IN_PROGRESS;
    private final List<ExecutionStateListener> _listeners = new ArrayList<>();

    /**
     * Transitions to a new state if valid.
     *
     * @param newState the desired state
     * @throws IllegalStateTransitionException if transition is invalid
     */
    public synchronized void transition(ExecutionState newState) {
        if (!isValidTransition(_currentState, newState)) {
            throw new IllegalStateTransitionException(
                String.format("Cannot transition from %s to %s",
                    _currentState, newState)
            );
        }

        ExecutionState oldState = _currentState;
        _currentState = newState;

        // Notify listeners
        notifyListeners(oldState, newState);
    }

    private boolean isValidTransition(ExecutionState from,
                                     ExecutionState to) {
        // Transition matrix
        return switch (from) {
            case NO_TASK_IN_PROGRESS ->
                to == STARTING_EXECUTION;
            case STARTING_EXECUTION ->
                to == INTER_BROKER_REPLICA_MOVEMENT || to == STOPPING_EXECUTION;
            case INTER_BROKER_REPLICA_MOVEMENT ->
                to == LEADER_MOVEMENT || to == STOPPING_EXECUTION;
            case LEADER_MOVEMENT ->
                to == NO_TASK_IN_PROGRESS || to == STOPPING_EXECUTION;
            case STOPPING_EXECUTION ->
                to == NO_TASK_IN_PROGRESS || to == STOPPED_WITH_ERROR;
            default -> false;
        };
    }

    public ExecutionState currentState() {
        return _currentState;
    }
}
```

**Key Methods:**
- `transition(newState)` - Change state with validation
- `isValidTransition()` - Check if transition allowed
- `registerListener()` - Listen for state changes

**Benefits:**
- Explicit state transition rules
- Easier to reason about execution flow
- Simple to test all transitions
- ~500 LOC

---

#### 4. **ThrottleManager**

**Responsibility:** Manage execution throttling

**Location:** `com.linkedin.kafka.cruisecontrol.executor.throttle.ThrottleManager`

```java
/**
 * Manages throttling of partition movements to prevent cluster overload.
 *
 * <p>Supports two types of throttling:
 * <ul>
 *   <li><b>Concurrency throttling:</b> Limits number of simultaneous movements</li>
 *   <li><b>Bandwidth throttling:</b> Limits replication bandwidth (via Kafka)</li>
 * </ul>
 */
public class ThrottleManager {
    private final int _maxConcurrentMovements;
    private final long _maxBandwidthBytesPerSec;
    private final AtomicInteger _ongoingMovements = new AtomicInteger(0);

    /**
     * Determines how many more tasks can be submitted given current throttle.
     *
     * @return number of additional tasks that can be submitted
     */
    public int availableCapacity() {
        int ongoing = _ongoingMovements.get();
        return Math.max(0, _maxConcurrentMovements - ongoing);
    }

    /**
     * Requests capacity to execute tasks.
     *
     * @param requestedTasks number of tasks wanting to start
     * @return number of tasks that can actually start (may be less)
     */
    public int requestCapacity(int requestedTasks) {
        int available = availableCapacity();
        int granted = Math.min(requestedTasks, available);

        if (granted > 0) {
            _ongoingMovements.addAndGet(granted);
        }

        return granted;
    }

    /**
     * Releases capacity when tasks complete.
     *
     * @param completedTasks number of tasks that finished
     */
    public void releaseCapacity(int completedTasks) {
        _ongoingMovements.addAndGet(-completedTasks);
    }

    /**
     * Gets Kafka replication throttle configuration.
     *
     * @return throttle config to apply to Kafka
     */
    public ReplicationThrottle getKafkaThrottle() {
        return new ReplicationThrottle(_maxBandwidthBytesPerSec);
    }
}
```

**Key Methods:**
- `availableCapacity()` - Check how many tasks can start
- `requestCapacity()` - Acquire throttle permits
- `releaseCapacity()` - Return permits when done
- `getKafkaThrottle()` - Get Kafka-level throttle config

**Benefits:**
- Isolated throttling logic
- Easy to test different throttle strategies
- Clean separation from execution logic
- ~400 LOC

---

#### 5. **KafkaExecutionClient**

**Responsibility:** Interact with Kafka AdminClient

**Location:** `com.linkedin.kafka.cruisecontrol.executor.kafka.KafkaExecutionClient`

```java
/**
 * Executes operations against Kafka cluster via AdminClient.
 *
 * <p>This class is the sole interface to Kafka AdminClient for execution
 * operations, making it easy to mock for testing.
 */
public class KafkaExecutionClient {
    private final AdminClient _adminClient;
    private final ExecutionMetrics _metrics;

    /**
     * Submits partition reassignment requests to Kafka.
     *
     * @param reassignments the partition reassignments to execute
     * @return future that completes when submission is done (not execution)
     * @throws ExecutionException if submission fails
     */
    public CompletableFuture<Void> submitReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {

        _metrics.recordApiCall("alterPartitionReassignments");

        return CompletableFuture.supplyAsync(() -> {
            try {
                AlterPartitionReassignmentsResult result =
                    _adminClient.alterPartitionReassignments(reassignments);

                // Wait for submission to complete
                result.all().get(30, TimeUnit.SECONDS);

                _metrics.recordApiSuccess("alterPartitionReassignments");
                return null;
            } catch (Exception e) {
                _metrics.recordApiFailure("alterPartitionReassignments");
                throw new ExecutionException("Failed to submit reassignments", e);
            }
        });
    }

    /**
     * Triggers leader election for specified partitions.
     *
     * @param partitions the partitions needing leader election
     * @return future that completes when elections finish
     */
    public CompletableFuture<Void> electLeaders(
            Collection<TopicPartition> partitions) {

        _metrics.recordApiCall("electLeaders");

        return CompletableFuture.supplyAsync(() -> {
            try {
                ElectLeadersResult result = _adminClient.electLeaders(
                    ElectionType.PREFERRED,
                    new HashSet<>(partitions)
                );

                result.all().get(30, TimeUnit.SECONDS);

                _metrics.recordApiSuccess("electLeaders");
                return null;
            } catch (Exception e) {
                _metrics.recordApiFailure("electLeaders");
                throw new ExecutionException("Failed to elect leaders", e);
            }
        });
    }

    /**
     * Queries current partition reassignment status.
     *
     * @param partitions partitions to check
     * @return map of partition to reassignment status
     */
    public Map<TopicPartition, PartitionReassignmentStatus>
            getReassignmentStatus(Collection<TopicPartition> partitions) {

        // Query Kafka for current reassignment state
        ListPartitionReassignmentsResult result =
            _adminClient.listPartitionReassignments(new HashSet<>(partitions));

        // Convert to our internal status representation
        return convertToStatus(result);
    }
}
```

**Key Methods:**
- `submitReassignments()` - Submit partition moves
- `electLeaders()` - Trigger leader elections
- `getReassignmentStatus()` - Query execution status
- `setThrottles()` - Apply throttle configuration

**Benefits:**
- Single place for Kafka API calls
- Easy to mock for unit tests
- Metrics for all Kafka operations
- ~500 LOC

---

#### 6. **ExecutionMetrics**

**Responsibility:** Track execution metrics and progress

**Location:** `com.linkedin.kafka.cruisecontrol.executor.metrics.ExecutionMetrics`

```java
/**
 * Tracks metrics for execution operations.
 */
public class ExecutionMetrics {
    private final MetricRegistry _registry;

    // Gauges
    private final Gauge<Integer> _tasksInProgress;
    private final Gauge<Integer> _tasksPending;
    private final Gauge<Integer> _tasksCompleted;

    // Histograms
    private final Histogram _taskDuration;
    private final Histogram _throughput;

    // Counters
    private final Counter _tasksSubmitted;
    private final Counter _tasksFailed;
    private final Counter _kafkaApiCalls;

    public void recordTaskSubmitted() {
        _tasksSubmitted.inc();
    }

    public void recordTaskCompleted(long durationMs) {
        _taskDuration.update(durationMs);
    }

    public void recordBytesTransferred(long bytes) {
        _throughput.update(bytes);
    }

    /**
     * Gets current execution progress as percentage.
     *
     * @return progress from 0.0 to 1.0
     */
    public double getProgress() {
        int total = _tasksCompleted.getValue() +
                   _tasksInProgress.getValue() +
                   _tasksPending.getValue();
        if (total == 0) return 0.0;
        return (double) _tasksCompleted.getValue() / total;
    }
}
```

**Key Methods:**
- `recordTaskSubmitted/Completed/Failed()` - Track task lifecycle
- `recordBytesTransferred()` - Track throughput
- `getProgress()` - Calculate completion percentage

**Benefits:**
- Centralized metrics logic
- Easy to add new metrics
- Clean separation from execution logic
- ~300 LOC

---

#### 7. **ExecutionTaskRepository**

**Responsibility:** Store and query execution tasks

**Location:** `com.linkedin.kafka.cruisecontrol.executor.task.ExecutionTaskRepository`

```java
/**
 * Repository for execution tasks.
 *
 * <p>Provides efficient storage and querying of tasks by various criteria.
 */
public class ExecutionTaskRepository {
    private final Map<String, ExecutionTask> _tasksById =
        new ConcurrentHashMap<>();

    private final Map<ExecutionState, Set<String>> _tasksByState =
        new ConcurrentHashMap<>();

    /**
     * Saves a task to the repository.
     */
    public void save(ExecutionTask task) {
        _tasksById.put(task.id(), task);
        _tasksByState
            .computeIfAbsent(task.state(), k -> ConcurrentHashMap.newKeySet())
            .add(task.id());
    }

    /**
     * Retrieves a task by ID.
     */
    public ExecutionTask get(String id) {
        return _tasksById.get(id);
    }

    /**
     * Finds all tasks in a given state.
     */
    public List<ExecutionTask> findByState(ExecutionState state) {
        Set<String> taskIds = _tasksByState.getOrDefault(
            state, Collections.emptySet());

        return taskIds.stream()
            .map(_tasksById::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Updates task state (must maintain index consistency).
     */
    public void updateState(String taskId, ExecutionState newState) {
        ExecutionTask task = _tasksById.get(taskId);
        if (task == null) return;

        // Remove from old state index
        _tasksByState.get(task.state()).remove(taskId);

        // Update task
        task.setState(newState);

        // Add to new state index
        _tasksByState
            .computeIfAbsent(newState, k -> ConcurrentHashMap.newKeySet())
            .add(taskId);
    }
}
```

**Key Methods:**
- `save()` - Store task
- `get()` - Retrieve by ID
- `findByState()` - Query by state
- `updateState()` - Change state and update indices

**Benefits:**
- Efficient task queries (O(1) by state)
- Clean separation of storage concerns
- Easy to test
- ~200 LOC

---

### New Executor (Simplified Facade)

```java
/**
 * Coordinates execution of optimization proposals.
 *
 * <p>This class is now a thin facade that delegates to specialized components.
 */
public class Executor {
    private final ExecutionPlanner _planner;
    private final ExecutionTaskManager _taskManager;
    private final ExecutionStateMachine _stateMachine;
    private final ThrottleManager _throttleManager;
    private final KafkaExecutionClient _kafkaClient;
    private final ExecutionMetrics _metrics;

    private final ScheduledExecutorService _monitor;

    /**
     * Executes the given proposals.
     *
     * @param proposals the optimization proposals to execute
     * @param options execution options
     * @throws ExecutionException if execution fails
     */
    public void executeProposals(Collection<ExecutionProposal> proposals,
                                ExecutionOptions options)
            throws ExecutionException {

        // 1. Plan execution
        ExecutionPlan plan = _planner.plan(proposals, options);

        // 2. Transition to executing state
        _stateMachine.transition(STARTING_EXECUTION);

        // 3. Execute Phase 1: Replica movements
        executeReplicaMovements(plan.replicaMovements());

        // 4. Execute Phase 2: Leadership changes
        executeLeadershipChanges(plan.leadershipChanges());

        // 5. Complete
        _stateMachine.transition(NO_TASK_IN_PROGRESS);
    }

    private void executeReplicaMovements(List<ReplicaMovementTask> tasks) {
        _stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT);

        // Submit tasks with throttling
        _taskManager.submitTasks(tasks);

        // Monitor until all complete
        while (hasInProgressTasks()) {
            // Check how many new tasks can start
            int capacity = _throttleManager.availableCapacity();

            if (capacity > 0) {
                // Start more pending tasks
                List<ExecutionTask> pending =
                    _taskManager.getTasksInState(PENDING);
                List<ExecutionTask> toStart = pending.subList(0,
                    Math.min(capacity, pending.size()));

                submitToKafka(toStart);
            }

            // Update task states based on Kafka cluster state
            updateTaskStates();

            Thread.sleep(10000); // Check every 10 seconds
        }
    }

    private void submitToKafka(List<ExecutionTask> tasks) {
        Map<TopicPartition, NewPartitionReassignment> reassignments =
            convertToReassignments(tasks);

        _kafkaClient.submitReassignments(reassignments)
            .thenRun(() -> {
                // Mark tasks as submitted
                tasks.forEach(t ->
                    _taskManager.updateTaskState(t.id(), IN_PROGRESS)
                );
            });
    }

    private void updateTaskStates() {
        // Get all in-progress tasks
        List<ExecutionTask> inProgress =
            _taskManager.getTasksInState(IN_PROGRESS);

        // Query Kafka for their status
        Set<TopicPartition> partitions = extractPartitions(inProgress);
        Map<TopicPartition, PartitionReassignmentStatus> statuses =
            _kafkaClient.getReassignmentStatus(partitions);

        // Update task states based on Kafka response
        for (ExecutionTask task : inProgress) {
            PartitionReassignmentStatus status =
                statuses.get(task.partition());

            if (status.isComplete()) {
                _taskManager.updateTaskState(task.id(), COMPLETED);
                _throttleManager.releaseCapacity(1);
            }
        }
    }

    // ... rest of facade methods (~300 LOC total)
}
```

---

## Implementation Plan

### Phase 1: Extract Utilities and Metrics (Week 1-2)

**Tasks:**
1. Create `ExecutionMetrics` class
2. Move all metrics tracking logic
3. Create `ExecutionTaskRepository` class
4. Extract utility methods

**Validation:**
- Existing tests still pass
- No behavior changes

---

### Phase 2: Extract Kafka Client (Week 3)

**Tasks:**
1. Create `KafkaExecutionClient` interface
2. Implement with existing AdminClient logic
3. Update Executor to use client
4. Write client tests (mocking AdminClient)

**Validation:**
- Integration tests pass with real Kafka
- Unit tests work with mock client

---

### Phase 3: Extract State Machine and Throttling (Week 4-5)

**Tasks:**
1. Create `ExecutionStateMachine` class
2. Create `ThrottleManager` class
3. Refactor Executor to use both
4. Add explicit state transition tests

**Validation:**
- All state transitions tested
- Throttling behavior unchanged

---

### Phase 4: Extract Task Management (Week 6-7)

**Tasks:**
1. Create `ExecutionTaskManager` class
2. Move task lifecycle logic
3. Refactor Executor to delegate
4. Test task transitions in isolation

**Validation:**
- Task state transitions correct
- Performance unchanged

---

### Phase 5: Extract Planning (Week 8)

**Tasks:**
1. Create `ExecutionPlanner` class
2. Move proposal-to-task conversion
3. Test planning logic independently

**Validation:**
- Plan generation correct
- Easy to test different strategies

---

### Phase 6: Integration and Cleanup (Week 9-10)

**Tasks:**
1. Simplify Executor to pure facade
2. Remove dead code
3. Update documentation
4. Performance testing

**Validation:**
- All tests green
- Performance same or better
- Code coverage maintained

---

## Testing Strategy

### Unit Tests

Each new class gets comprehensive unit tests:

```java
// Example: ThrottleManagerTest
@Test
public void testRequestCapacity() {
    ThrottleManager throttle = new ThrottleManager(5, 1_000_000);

    // Can get up to max
    assertEquals(5, throttle.requestCapacity(10));
    assertEquals(0, throttle.requestCapacity(1)); // None left

    // Release and request again
    throttle.releaseCapacity(2);
    assertEquals(2, throttle.requestCapacity(10));
}

@Test
public void testThrottleUnderConcurrentLoad() {
    ThrottleManager throttle = new ThrottleManager(10, 1_000_000);

    // Simulate 20 threads requesting capacity
    CountDownLatch latch = new CountDownLatch(20);
    AtomicInteger totalGranted = new AtomicInteger(0);

    for (int i = 0; i < 20; i++) {
        new Thread(() -> {
            int granted = throttle.requestCapacity(1);
            totalGranted.addAndGet(granted);
            latch.countDown();
        }).start();
    }

    latch.await();

    // Should never exceed max
    assertTrue(totalGranted.get() <= 10);
}
```

### Integration Tests

Test end-to-end execution with real Kafka (TestContainers):

```java
@Test
public void testExecutionWithRefactoredComponents() {
    // Setup: Create test cluster with 3 brokers
    KafkaCluster cluster = new KafkaCluster(3);

    // Create proposals to move partitions
    List<ExecutionProposal> proposals = createTestProposals();

    // Execute
    Executor executor = createExecutor(cluster);
    executor.executeProposals(proposals, new ExecutionOptions());

    // Verify: Partitions moved correctly
    assertPartitionsMovedCorrectly(cluster, proposals);
}
```

---

## Migration Strategy

### Backward Compatibility

**Strategy:** Refactor incrementally, maintaining external API

```java
// External API unchanged
public class Executor {
    public void executeProposals(...) { // Same signature
        // Now delegates to new components
    }

    public ExecutorState state() { // Same signature
        return _stateMachine.currentState();
    }
}
```

**Result:** Zero breaking changes for users

### Rollout Plan

1. **Week 1-5:** Internal refactoring only, all tests pass
2. **Week 6-7:** Code review and validation
3. **Week 8:** Deploy to test environment
4. **Week 9:** Deploy to production (canary → full rollout)
5. **Week 10:** Monitor and optimize

---

## Success Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Executor.java LOC | 3,500 | 300 | 91% reduction |
| Largest class size | 3,500 | 600 | 83% reduction |
| Unit test coverage | 60% | 85% | +25% |
| New engineer onboarding | 2-3 weeks | 3-5 days | 75% faster |
| Average method complexity | 12 | 5 | 58% reduction |

---

## Risks and Mitigations

### Risk 1: Introduce Bugs During Refactoring

**Mitigation:**
- Comprehensive test coverage before starting
- Refactor incrementally, validate at each step
- Use feature flags for gradual rollout

### Risk 2: Performance Regression

**Mitigation:**
- Benchmark before/after
- Profile hot paths
- No additional object allocations in hot loops

### Risk 3: Team Resistance

**Mitigation:**
- Clear documentation of benefits
- Involve team in design review
- Demonstrate improved testability early

---

## Effort Estimate

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: Utilities & Metrics | 2 weeks | 10 days |
| Phase 2: Kafka Client | 1 week | 5 days |
| Phase 3: State & Throttle | 2 weeks | 10 days |
| Phase 4: Task Management | 2 weeks | 10 days |
| Phase 5: Planning | 1 week | 5 days |
| Phase 6: Integration | 2 weeks | 10 days |
| **Total** | **10 weeks** | **50 days** |

---

## References

- Martin Fowler, *Refactoring: Improving the Design of Existing Code*
- Robert Martin, *Clean Code*
- Michael Feathers, *Working Effectively with Legacy Code*
- [Cruise Control Executor.java](https://github.com/linkedin/cruise-control/blob/master/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/executor/Executor.java)

---

**Priority:** P1 (Important for maintainability)
**Effort:** 30-40 dev-days
**Dependencies:** None
**Breaking Changes:** None (internal refactoring only)
