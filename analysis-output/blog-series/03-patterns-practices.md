# Design Patterns and Practices in Cruise Control

**Part 3 of 6** in the Cruise Control Deep Dive Series

**Reading Time:** ~10 minutes
**Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## What You'll Learn

- Real-world applications of the Strategy and Template Method patterns
- How the plugin architecture achieves extensibility
- Error handling and resilience patterns
- Code organization strategies in a large codebase

## The Strategy Pattern: Pluggability Everywhere

Cruise Control uses the Strategy pattern extensively to allow runtime customization.

### Pattern 1: Metric Sampling Strategies

**Interface:**
```java
// MetricSampler.java
public interface MetricSampler extends CruiseControlConfigurable, AutoCloseable {
    Samples getSamples(MetricSamplerOptions options) throws SamplingException;
}
```

**Implementations:**
1. **CruiseControlMetricsReporterSampler** - Default, reads from Kafka topic
2. **PrometheusMetricSampler** - Reads from Prometheus

**Why this works:** You can swap metric sources by changing one config line:
```properties
metric.sampler.class=com.linkedin.kafka.cruisecontrol.monitor.sampling.PrometheusMetricSampler
```

### Pattern 2: Execution Strategies

**Interface:**
```java
// ReplicaMovementStrategy.java
public interface ReplicaMovementStrategy {
    List<ExecutionTask> applyStrategy(List<ExecutionTask> tasks, 
                                       ClusterModel clusterModel);
}
```

**9 Built-in Strategies:**
- `BaseReplicaMovementStrategy` - FIFO ordering
- `PostponeUrpReplicaMovementStrategy` - Deprioritize under-replicated partitions
- `PrioritizeLargeReplicaMovementStrategy` - Move large replicas first
- `PrioritizeMinIsrWithOfflineReplicasStrategy` - Prioritize partitions at risk

**Real example:**
```java
// From PostponeUrpReplicaMovementStrategy.java
@Override
public List<ExecutionTask> applyStrategy(List<ExecutionTask> tasks, 
                                          ClusterModel clusterModel) {
    // Separate URP from healthy partitions
    List<ExecutionTask> urpTasks = new ArrayList<>();
    List<ExecutionTask> healthyTasks = new ArrayList<>();
    
    for (ExecutionTask task : tasks) {
        if (clusterModel.partition(task.topicPartition()).isUnderReplicated()) {
            urpTasks.add(task);
        } else {
            healthyTasks.add(task);
        }
    }
    
    // Return healthy first, URP last
    healthyTasks.addAll(urpTasks);
    return healthyTasks;
}
```

**Why:** Moving URP partitions is risky (they're already vulnerable). Do healthy partitions first.

### Pattern 3: Anomaly Notification Strategies

**Interface:**
```java
// AnomalyNotifier.java
public interface AnomalyNotifier extends CruiseControlConfigurable {
    AnomalyNotificationResult onBrokerFailure(BrokerFailures failures);
    AnomalyNotificationResult onGoalViolation(GoalViolations violations);
    AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly anomaly);
    // ... other anomaly types
}
```

**Implementations:**
- `SelfHealingNotifier` - Auto-fix or alert
- `SlackSelfHealingNotifier` - Post to Slack
- `MSTeamsSelfHealingNotifier` - Post to Microsoft Teams
- `AlertaSelfHealingNotifier` - Send to Alerta monitoring

**Pattern:** Same anomaly detection, different notification channels

## The Template Method Pattern: Consistent Structure

We saw this in `AbstractGoal` (Post 2). Let's see it in anomaly detection:

```java
// AbstractAnomalyDetector.java
public abstract class AbstractAnomalyDetector implements Runnable {
    
    @Override
    public void run() {
        while (!_shutdown) {
            try {
                // Template method: subclasses provide detection logic
                Anomaly anomaly = detect();
                
                if (anomaly != null) {
                    // Common handling logic
                    _anomalies.add(anomaly);
                }
                
                // Wait before next detection cycle
                Thread.sleep(detectionIntervalMs());
            } catch (Exception e) {
                handleException(e);
            }
        }
    }
    
    // Hook methods for subclasses
    protected abstract Anomaly detect();
    protected abstract long detectionIntervalMs();
}
```

**Subclasses fill in:**
- `KafkaBrokerFailureDetector.detect()` - Check for dead brokers
- `DiskFailureDetector.detect()` - Check for failed disks
- `GoalViolationDetector.detect()` - Run goals to find violations

**Benefit:** All detectors follow the same lifecycle, centralized error handling

## The Facade Pattern: Simplifying Complexity

`KafkaCruiseControl` is a textbook Facade:

```java
// KafkaCruiseControl.java
public class KafkaCruiseControl {
    private final LoadMonitor _loadMonitor;
    private final GoalOptimizer _goalOptimizer;
    private final Executor _executor;
    private final AnomalyDetectorManager _anomalyDetectorManager;
    private final Provisioner _provisioner;
    
    // Simplified API for REST layer
    public OptimizerResult getProposals(...) {
        ClusterModel model = _loadMonitor.clusterModel(...);
        return _goalOptimizer.optimizations(model, ...);
    }
    
    public void executeProposals(...) {
        // Coordinate load monitor + executor
        _executor.executeProposals(...);
    }
    
    public ClusterState state() {
        // Aggregate state from all components
        return new ClusterState(
            _loadMonitor.state(),
            _goalOptimizer.state(),
            _executor.state(),
            _anomalyDetectorManager.state()
        );
    }
}
```

**Without facade:** REST handlers would need to coordinate 5 components
**With facade:** REST handlers call simple methods on one object

## Plugin Architecture: Making Everything Extensible

### The CruiseControlConfigurable Interface

Every pluggable component implements this:

```java
public interface CruiseControlConfigurable {
    void configure(Map<String, ?> configs);
}
```

### Configuration-Driven Instantiation

```java
// From KafkaCruiseControlConfig.java
public <T> T getConfiguredInstance(String key, Class<T> type) {
    String className = getString(key);
    
    // Reflection-based instantiation
    Class<?> clazz = Class.forName(className);
    T instance = (T) clazz.getDeclaredConstructor().newInstance();
    
    // Configure with merged config
    if (instance instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) instance).configure(originals());
    }
    
    return instance;
}
```

### Plugin Points

| Plugin Type | Interface | Default Implementation |
|-------------|-----------|------------------------|
| Goals | `Goal` | 28 built-in goals |
| Metric Sampler | `MetricSampler` | `CruiseControlMetricsReporterSampler` |
| Sample Store | `SampleStore` | `KafkaSampleStore` |
| Anomaly Notifier | `AnomalyNotifier` | `SelfHealingNotifier` |
| Execution Strategy | `ReplicaMovementStrategy` | `BaseReplicaMovementStrategy` |
| Capacity Resolver | `BrokerCapacityConfigResolver` | `BrokerCapacityConfigFileResolver` |
| Topic Config Provider | `TopicConfigProvider` | `KafkaAdminTopicConfigProvider` |
| Security Provider | `SecurityProvider` | `BasicSecurityProvider` |

## Error Handling Patterns

### Pattern 1: Graceful Degradation

When metrics are incomplete, Cruise Control doesn't fail—it warns:

```java
// From LoadMonitor.java
double monitoredRatio = monitoredPartitions / (double) totalPartitions;

if (monitoredRatio < _minValidPartitionRatio) {
    LOG.warn("Only {}% of partitions monitored, need {}%",
             monitoredRatio * 100, _minValidPartitionRatio * 100);
    // Still return model, but mark as incomplete
    clusterModel.setMonitoredPartitionsRatio(monitoredRatio);
}
```

Users can decide: accept proposals with 90% coverage, or wait for 95%?

### Pattern 2: Fail-Fast Validation

Execution validates thoroughly before starting:

```java
// From Executor.java:863-888
private void initProposalExecution(...) throws OngoingExecutionException {
    // Validate no execution in progress
    if (!_noOngoingExecutionSemaphore.tryAcquire()) {
        throw new OngoingExecutionException("Already executing");
    }
    
    // Validate cluster state is current
    if (clusterModel.generation() < _loadMonitor.latestGeneration()) {
        throw new IllegalStateException("Stale cluster model");
    }
    
    // Validate proposals are non-empty
    if (proposals.isEmpty()) {
        throw new IllegalArgumentException("No proposals to execute");
    }
    
    // Only now proceed with execution
}
```

**Benefit:** Fail before we start moving partitions, not during

### Pattern 3: Exception Translation

Low-level exceptions become domain exceptions:

```java
// From MetricSamplerManager.java
try {
    Samples samples = _metricSampler.getSamples(options);
} catch (KafkaException e) {
    throw new SamplingException("Failed to sample metrics", e);
} catch (TimeoutException e) {
    throw new NotEnoughValidWindowsException("Sampling timeout", e);
}
```

**Why:** REST API can return appropriate HTTP codes (503 for timeout, 500 for errors)

## Resilience Patterns

### Pattern 1: Circuit Breaker (Implicit)

Goal optimization has a timeout:

```java
// From GoalOptimizer.java
long deadline = System.currentTimeMillis() + optimizationTimeoutMs;

for (Goal goal : goals) {
    if (System.currentTimeMillis() > deadline) {
        throw new TimeoutException("Goal optimization timeout");
    }
    goal.optimize(clusterModel, ...);
}
```

**Prevents:** A misbehaving goal hanging the system

### Pattern 2: Bulkhead (Thread Pools)

Components run in isolated thread pools:

```java
// Anomaly detectors: 7 independent threads
_brokerFailureDetector = new Thread(new BrokerFailureDetector(...));
_diskFailureDetector = new Thread(new DiskFailureDetector(...));
_goalViolationDetector = new Thread(new GoalViolationDetector(...));
// ... 4 more

// If one detector crashes, others continue
```

### Pattern 3: Retry with Backoff

Execution retries stuck tasks:

```java
// From Executor.java
if (noTasksCompletedButSomeInExecution) {
    // Tasks are stuck, retry
    int backoffMs = 1000 * (int) Math.pow(2, retryAttempt);
    Thread.sleep(backoffMs);
    retryAttempt++;
    maybeReexecuteInterBrokerReplicaTasks();
}
```

## Code Organization Strategies

### Strategy 1: Package by Feature

```
analyzer/
  ├── goals/              # All goal implementations
  ├── kafkaassigner/      # Kafka assigner compatibility
  ├── AnalyzerUtils.java
  └── GoalOptimizer.java
```

Not "models", "services", "controllers"—organized by domain concept.

### Strategy 2: Separation by Layer

```
servlet/
  ├── handler/       # Request handling logic
  ├── parameters/    # Request parameter validation
  ├── response/      # Response formatting
  └── security/      # Authentication
```

Clear separation of concerns within the API layer.

### Strategy 3: Common Utilities in One Place

```
common/
  ├── Resource.java                 # Resource enum (CPU, DISK, etc.)
  ├── KafkaCruiseControlThreadFactory.java
  ├── MetadataClient.java
  └── Utils.java
```

Avoids duplication, clear dependency direction.

## Testing Patterns

### Pattern 1: Deterministic Test Clusters

```java
// From DeterministicCluster.java (3000 LOC test utility)
public static ClusterModel smallClusterModel() {
    // Always returns same cluster:
    // - 3 brokers in 2 racks
    // - 12 partitions with known load
    // - Deterministic replica placement
    return clusterModel(SMALL_CLUSTER_CONFIG);
}
```

**Benefit:** Tests are reproducible, easy to debug

### Pattern 2: Builder Pattern for Test Data

```java
BrokerCapacityInfo capacity = new BrokerCapacityInfo.Builder()
    .cpu(100.0)
    .disk(1000000.0)
    .networkIn(10000.0)
    .networkOut(10000.0)
    .build();
```

### Pattern 3: Test Doubles via Interfaces

All components are interfaces → easy to mock:

```java
@Test
public void testGoalOptimizer() {
    // Mock LoadMonitor
    LoadMonitor mockMonitor = EasyMock.createMock(LoadMonitor.class);
    EasyMock.expect(mockMonitor.clusterModel(...))
            .andReturn(DeterministicCluster.smallClusterModel());
    
    // Test in isolation
    GoalOptimizer optimizer = new GoalOptimizer(mockMonitor, ...);
}
```

## Key Takeaways

1. **Strategy Pattern enables customization:** Swap implementations via configuration
2. **Template Method ensures consistency:** All goals/detectors follow same pattern
3. **Facade simplifies coordination:** REST layer doesn't manage 5 components
4. **Plugin architecture via interfaces:** Almost everything is pluggable
5. **Error handling is layered:** Validate early, degrade gracefully, translate exceptions
6. **Resilience through isolation:** Circuit breakers, bulkheads, retries
7. **Code organization by feature:** Not by layer, by domain concept

## Next Steps

1. **Study a design pattern book:** "Design Patterns" (Gang of Four, 1994) or "Head First Design Patterns"
2. **Explore Cruise Control plugins:** Try implementing a custom goal or notifier (Post 4)
3. **Review error handling:** Follow a request through error cases
4. **Next in series:** Post 4 shows how to extend Cruise Control

---

**Next Post:** [Extending and Integrating Cruise Control →](./04-extending-integrating.md)

*This post is part of a 6-part series analyzing Cruise Control for Apache Kafka. All code references are based on commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6).*
