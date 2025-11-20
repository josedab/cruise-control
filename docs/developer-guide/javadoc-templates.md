# Javadoc Templates and Examples

This document provides templates and examples for writing high-quality Javadoc documentation in Cruise Control.

## General Guidelines

- **All public classes and methods MUST have Javadoc**
- **Use HTML tags for formatting:** `<p>`, `<ul>`, `<li>`, `<pre>`, `<code>`, etc.
- **Use `@param`, `@return`, `@throws` tags consistently**
- **Include examples where helpful**
- **Document performance characteristics for critical code**
- **Explain non-obvious design decisions**

## Class Documentation Templates

### Template for Goal Classes

```java
/**
 * [One-line summary of what this goal optimizes]
 *
 * <p>[Detailed description of the optimization strategy and when it applies]
 *
 * <h3>Goal Behavior</h3>
 * <p>[Explanation of how the goal works, what it optimizes for, and the
 * algorithm it uses]
 *
 * <h3>Priority</h3>
 * <p>Priority: [X] - [Explanation of when this goal should run relative
 * to other goals and why this priority was chosen]
 *
 * <h3>Hard vs Soft Goal</h3>
 * <p>[Hard/Soft] - [Explanation of whether this goal must be satisfied
 * (hard) or is best-effort (soft), and the rationale]
 *
 * <h3>Example Scenario</h3>
 * <pre>
 * Before optimization:
 *   Broker 1: [state description]
 *   Broker 2: [state description]
 *   Broker 3: [state description]
 *
 * After optimization:
 *   Broker 1: [improved state]
 *   Broker 2: [improved state]
 *   Broker 3: [improved state]
 * </pre>
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code config.property.name} - Description and default value</li>
 *   <li>{@code another.property} - Description</li>
 * </ul>
 *
 * <h3>Performance Characteristics</h3>
 * <ul>
 *   <li>Time Complexity: O([complexity with explanation])</li>
 *   <li>Space Complexity: O([complexity])</li>
 *   <li>Typical execution time: [time] for [cluster size]</li>
 * </ul>
 *
 * <h3>Related Goals</h3>
 * <ul>
 *   <li>{@link RelatedGoal1} - [How they interact]</li>
 *   <li>{@link RelatedGoal2} - [Relationship]</li>
 * </ul>
 *
 * @see AbstractGoal
 * @see GoalOptimizer
 * @since 2.0.0
 */
public class MyCustomGoal extends AbstractGoal {
    // Implementation
}
```

### Real Example: RackAwareGoal

```java
/**
 * Ensures partition replicas are distributed across different racks for fault tolerance.
 *
 * <p>This goal verifies that no partition has multiple replicas on brokers in the same rack.
 * If violations are found, it relocates replicas to brokers in different racks while respecting
 * capacity constraints.
 *
 * <h3>Goal Behavior</h3>
 * <p>The goal scans all partitions and identifies those with multiple replicas in the same rack.
 * For each violation, it selects the replica with the lowest load and moves it to a broker in
 * a different rack. The selection algorithm prioritizes moving from high-loaded brokers to
 * low-loaded brokers to maintain balance.
 *
 * <h3>Priority</h3>
 * <p>Priority: 0 - This is the highest priority hard goal because rack awareness is fundamental
 * for fault tolerance. If a rack fails, the cluster must remain available.
 *
 * <h3>Hard vs Soft Goal</h3>
 * <p>Hard - This goal MUST be satisfied. Clusters without rack-aware replica placement risk
 * complete data loss if a rack fails.
 *
 * <h3>Example Scenario</h3>
 * <pre>
 * Before optimization:
 *   Partition topic-0:
 *     Replica 1: Broker 0 (rack-1)
 *     Replica 2: Broker 1 (rack-1)  ← Violation!
 *     Replica 3: Broker 2 (rack-2)
 *
 * After optimization:
 *   Partition topic-0:
 *     Replica 1: Broker 0 (rack-1)
 *     Replica 2: Broker 3 (rack-3)  ← Moved to different rack
 *     Replica 3: Broker 2 (rack-2)
 * </pre>
 *
 * <h3>Configuration</h3>
 * <p>No specific configuration required. Rack information is read from broker metadata.
 *
 * <h3>Performance Characteristics</h3>
 * <ul>
 *   <li>Time Complexity: O(partitions × replicas × racks)</li>
 *   <li>Typical execution time: 5-10 seconds for 100K partitions</li>
 *   <li>Memory: Minimal overhead (only tracks rack assignments)</li>
 * </ul>
 *
 * @see AbstractGoal
 * @see ReplicaCapacityGoal
 */
public class RackAwareGoal extends AbstractGoal {
    // Implementation
}
```

### Template for Core Component Classes

```java
/**
 * [One-line summary of component's purpose]
 *
 * <p>[Detailed description of what this component does, its role in the system,
 * and how it interacts with other components]
 *
 * <h3>Responsibilities</h3>
 * <ul>
 *   <li>[Responsibility 1]</li>
 *   <li>[Responsibility 2]</li>
 *   <li>[Responsibility 3]</li>
 * </ul>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>[Initialization step]</li>
 *   <li>[Runtime behavior]</li>
 *   <li>[Shutdown process]</li>
 * </ol>
 *
 * <h3>Thread Safety</h3>
 * <p>[Description of thread safety guarantees, synchronization strategy,
 * and any concurrent access constraints]
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code config.property} - Description</li>
 * </ul>
 *
 * @see RelatedClass1
 * @see RelatedClass2
 */
public class ComponentClass {
    // Implementation
}
```

### Real Example: LoadMonitor

```java
/**
 * Monitors Kafka cluster load and builds an in-memory ClusterModel.
 *
 * <p>The LoadMonitor continuously collects metrics from Kafka brokers via the
 * {@code __CruiseControlMetrics} topic, aggregates them into time windows, and
 * maintains an up-to-date ClusterModel representing the current and historical
 * cluster state. This model is used by the GoalOptimizer to generate rebalance
 * proposals.
 *
 * <h3>Responsibilities</h3>
 * <ul>
 *   <li>Consume and aggregate broker and partition metrics</li>
 *   <li>Build and maintain in-memory ClusterModel</li>
 *   <li>Provide cluster state snapshots to other components</li>
 *   <li>Detect and report metrics collection issues</li>
 * </ul>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li><b>Initialization:</b> Connects to Kafka, creates metric consumers,
 *       initializes sample stores</li>
 *   <li><b>Runtime:</b> Runs background thread that updates ClusterModel
 *       every 120 seconds (configurable)</li>
 *   <li><b>Shutdown:</b> Stops consumers, flushes pending samples, releases
 *       resources</li>
 * </ol>
 *
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe. ClusterModel access is synchronized using
 * read-write locks. Metric collection happens on a dedicated background thread.
 * Multiple threads can safely call {@link #clusterModel()} concurrently.
 *
 * <h3>Performance Characteristics</h3>
 * <ul>
 *   <li>ClusterModel build time: O(brokers × partitions)</li>
 *   <li>Memory usage: ~250KB per replica × replication factor × partitions</li>
 *   <li>Typical build time: 30-60 seconds for 1000 brokers, 100K partitions</li>
 * </ul>
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code partition.metrics.window.ms} - Metric window size (default: 3600000)</li>
 *   <li>{@code num.partition.metrics.windows} - Number of windows to retain (default: 168)</li>
 *   <li>{@code metadata.max.age.ms} - Metadata refresh interval (default: 300000)</li>
 * </ul>
 *
 * @see ClusterModel
 * @see MetricSampleAggregator
 */
public class LoadMonitor {
    // Implementation
}
```

## Method Documentation Templates

### Template for Complex Public Methods

```java
/**
 * [One-line summary of what the method does]
 *
 * <p>[Detailed description explaining the purpose, behavior, and any important
 * context. Include information about side effects, state modifications, and
 * interactions with other components.]
 *
 * <h4>Algorithm</h4>
 * <ol>
 *   <li>[Step 1 description]</li>
 *   <li>[Step 2 description]</li>
 *   <li>[Step 3 description]</li>
 * </ol>
 *
 * <h4>Performance</h4>
 * <ul>
 *   <li>Time Complexity: O([complexity])</li>
 *   <li>Space Complexity: O([complexity])</li>
 *   <li>[Any performance notes or optimization details]</li>
 * </ul>
 *
 * <h4>Example</h4>
 * <pre>{@code
 * // Example usage
 * ClusterModel model = loadMonitor.clusterModel();
 * OptimizationResult result = optimizer.optimize(model, goals, options);
 * }</pre>
 *
 * @param param1 [description including constraints, valid ranges, null behavior]
 * @param param2 [description]
 * @return [detailed description of return value, including null conditions]
 * @throws SpecificException if [specific condition that triggers exception]
 * @throws AnotherException when [another error condition]
 * @see RelatedMethod
 */
public ReturnType methodName(Type1 param1, Type2 param2)
    throws SpecificException, AnotherException {
    // Implementation
}
```

### Real Example: GoalOptimizer.optimize()

```java
/**
 * Optimizes the cluster according to the specified goals.
 *
 * <p>This method executes each goal in priority order, allowing each to modify
 * the ClusterModel to achieve its optimization objective. Goals may revert
 * changes made by previous goals if those changes violate the current goal's
 * requirements. The final ClusterModel represents the optimal state according
 * to all goals.
 *
 * <p>The method creates a working copy of the ClusterModel to avoid modifying
 * the original. After optimization completes, it generates ExecutionProposals
 * by computing the diff between the initial and final states.
 *
 * <h4>Algorithm</h4>
 * <ol>
 *   <li>Clone the provided ClusterModel</li>
 *   <li>For each goal in priority order:
 *     <ul>
 *       <li>Execute goal.optimize(clusterModel)</li>
 *       <li>Validate goal satisfaction</li>
 *       <li>Track statistics (num movements, load changes, etc.)</li>
 *     </ul>
 *   </li>
 *   <li>Generate ExecutionProposals by diffing initial vs. final model</li>
 *   <li>Return OptimizerResult with proposals and statistics</li>
 * </ol>
 *
 * <h4>Performance</h4>
 * <ul>
 *   <li>Time Complexity: O(goals × brokers × partitions)</li>
 *   <li>Space Complexity: O(brokers × partitions) - for ClusterModel copy</li>
 *   <li>Typical execution time: 2-5 minutes for 28 goals, 1000 brokers, 100K partitions</li>
 *   <li>Memory: 2x ClusterModel size during optimization</li>
 * </ul>
 *
 * <h4>Example</h4>
 * <pre>{@code
 * List<Goal> goals = Arrays.asList(
 *     new RackAwareGoal(),
 *     new ReplicaCapacityGoal(),
 *     new DiskCapacityGoal()
 * );
 * OptimizationOptions options = new OptimizationOptions.Builder()
 *     .excludedBrokers(Set.of(1, 2))
 *     .build();
 * OptimizerResult result = optimizer.optimize(clusterModel, goals, options);
 * System.out.println("Proposals: " + result.proposals().size());
 * }</pre>
 *
 * @param clusterModel the current cluster state to optimize (not modified)
 * @param goals the ordered list of goals to satisfy, must not be empty
 * @param options optimization options such as excluded brokers, must not be null
 * @return optimization result containing proposals, statistics, and goal violations
 * @throws OptimizationFailureException if a hard goal cannot be satisfied
 * @throws IllegalArgumentException if goals list is empty or options is null
 * @see Goal
 * @see OptimizerResult
 */
public OptimizerResult optimize(ClusterModel clusterModel,
                                List<Goal> goals,
                                OptimizationOptions options)
    throws OptimizationFailureException {
    // Implementation
}
```

### Template for Simple Getters/Setters

```java
/**
 * Returns [what is returned].
 *
 * @return [description including null behavior]
 */
public Type getProperty() {
    return _property;
}

/**
 * Sets [what is being set] to [new value].
 *
 * @param property [description including constraints and validation]
 * @throws IllegalArgumentException if [validation failure condition]
 */
public void setProperty(Type property) {
    _property = property;
}
```

## Special Sections

### Performance Documentation

When documenting performance-critical code, always include:

```java
/**
 * <h4>Performance Characteristics</h4>
 * <ul>
 *   <li>Time Complexity: O(n log n) where n = number of partitions</li>
 *   <li>Space Complexity: O(n) for temporary storage</li>
 *   <li>Typical execution time: 50ms for 10K partitions</li>
 *   <li>Bottleneck: Sorting operation at line 145</li>
 *   <li>Optimization: Uses parallel streams for large inputs (n > 1000)</li>
 * </ul>
 */
```

### Thread Safety Documentation

For classes with concurrency concerns:

```java
/**
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe. All public methods are synchronized on the
 * internal lock. The {@link #update()} method can be called concurrently with
 * {@link #get()}, but multiple calls to {@link #update()} are serialized.
 *
 * <p><b>Concurrency Note:</b> Avoid holding references to returned ClusterModel
 * objects across calls, as they may become stale if another thread calls
 * {@link #update()}.
 */
```

### State Machine Documentation

For state machines:

```java
/**
 * <h3>State Transitions</h3>
 * <pre>
 * NO_TASK_IN_PROGRESS → STARTING_EXECUTION  (via executeProposals())
 * STARTING_EXECUTION → INTER_BROKER_MOVEMENT (when tasks initialized)
 * INTER_BROKER_MOVEMENT → LEADER_MOVEMENT    (when movements complete)
 * LEADER_MOVEMENT → NO_TASK_IN_PROGRESS      (when leaders elected)
 * [any state] → STOPPING_EXECUTION           (via stopExecution())
 * STOPPING_EXECUTION → STOPPED_WITH_ERROR    (on error)
 * </pre>
 */
```

## Common Mistakes to Avoid

### ❌ Too Vague

```java
/**
 * Optimizes the cluster.
 */
public void optimize() { }
```

### ✅ Good

```java
/**
 * Optimizes the cluster by rebalancing partitions across brokers according
 * to configured goals.
 *
 * <p>This method generates partition movement proposals that improve cluster
 * balance while respecting capacity constraints and rack awareness.
 *
 * @throws OptimizationFailureException if hard goals cannot be satisfied
 */
public void optimize() { }
```

### ❌ Missing Parameter Descriptions

```java
/**
 * Creates a new executor.
 *
 * @param config
 * @param time
 */
public Executor(Config config, Time time) { }
```

### ✅ Good

```java
/**
 * Creates a new executor for applying partition movements to the Kafka cluster.
 *
 * @param config the Cruise Control configuration containing executor settings
 *               such as concurrency limits and throttle rates, must not be null
 * @param time the time implementation for scheduling and timeouts, must not be null
 * @throws IllegalArgumentException if config or time is null
 */
public Executor(Config config, Time time) { }
```

### ❌ No Examples for Complex APIs

```java
/**
 * Relocates a replica from source broker to destination broker.
 */
public void relocateReplica(TopicPartition tp, int source, int dest) { }
```

### ✅ Good

```java
/**
 * Relocates a replica from source broker to destination broker.
 *
 * <p>This method modifies the ClusterModel in-place by moving the specified
 * replica and updating broker loads accordingly.
 *
 * <h4>Example</h4>
 * <pre>{@code
 * TopicPartition tp = new TopicPartition("my-topic", 0);
 * clusterModel.relocateReplica(tp, /* from */ 1, /* to */ 3);
 * }</pre>
 *
 * @param tp the topic partition to move, must exist in the model
 * @param source the source broker ID, must currently host this replica
 * @param dest the destination broker ID, must be alive and have capacity
 * @throws IllegalArgumentException if replica doesn't exist or brokers invalid
 */
public void relocateReplica(TopicPartition tp, int source, int dest) { }
```

## Related Documentation

- [Contributing Guide](contributing.md)
- [Code Style Guide](building.md)
- [Oracle Javadoc Guidelines](https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html)
