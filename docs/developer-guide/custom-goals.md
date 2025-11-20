# Creating Custom Goals

This guide walks through implementing a custom goal that balances partitions based on a custom metric.

## Overview

Goals are the core of Cruise Control's optimization engine. Each goal represents an optimization objective, such as balancing disk usage, ensuring rack awareness, or distributing leadership.

## When to Create a Custom Goal

Create a custom goal when:
- You have a unique metric to balance (e.g., custom business metric)
- Built-in goals don't meet your requirements
- You want to enforce custom constraints (e.g., partition placement rules)

## Goal Basics

### Goal Types

**Hard Goals:**
- MUST be satisfied
- If violated, cluster is considered unhealthy
- Examples: `RackAwareGoal`, `ReplicaCapacityGoal`

**Soft Goals:**
- Best-effort optimization
- Can be violated if necessary
- Examples: `DiskUsageDistributionGoal`, `CpuUsageDistributionGoal`

### Goal Priority

Goals execute in priority order (lower number = higher priority):
1. Hard goals typically have priority 0-10
2. Soft goals typically have priority 11-100

## Step 1: Extend AbstractGoal

Create a new class extending `AbstractGoal`:

```java
package com.example.cruisecontrol.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.AbstractGoal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.*;

/**
 * Balances partitions based on message count per broker.
 *
 * <p>This goal ensures that all brokers handle a similar number of messages,
 * which can be important for workloads where message processing cost dominates
 * over data size.
 *
 * <h3>Goal Behavior</h3>
 * <p>The goal calculates the total message count for each broker (sum of all
 * partition message rates) and moves partitions from high-message-count brokers
 * to low-message-count brokers until the distribution is balanced.
 *
 * <h3>Priority</h3>
 * <p>Priority: 50 - This is a soft goal that runs after resource capacity goals
 * but before fine-grained distribution goals.
 *
 * <h3>Hard vs Soft Goal</h3>
 * <p>Soft - The goal will try to balance message counts but won't fail if
 * perfect balance cannot be achieved.
 *
 * <h3>Example Scenario</h3>
 * <pre>
 * Before:
 *   Broker 1: 1M messages/sec
 *   Broker 2: 500K messages/sec
 *   Broker 3: 200K messages/sec
 *
 * After:
 *   Broker 1: 600K messages/sec
 *   Broker 2: 550K messages/sec
 *   Broker 3: 550K messages/sec
 * </pre>
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code message.count.balance.threshold} - Max allowed deviation from average (default: 1.5)</li>
 * </ul>
 *
 * <h3>Performance Characteristics</h3>
 * <ul>
 *   <li>Time Complexity: O(brokers × partitions)</li>
 *   <li>Typical execution time: 10-30 seconds for 1000 brokers</li>
 * </ul>
 */
public class MessageCountBalanceGoal extends AbstractGoal {
  private static final double DEFAULT_THRESHOLD = 1.5;
  private final double balanceThreshold;

  public MessageCountBalanceGoal() {
    this(DEFAULT_THRESHOLD);
  }

  public MessageCountBalanceGoal(double balanceThreshold) {
    this.balanceThreshold = balanceThreshold;
  }

  @Override
  public String name() {
    return "MessageCountBalanceGoal";
  }

  @Override
  public boolean isHardGoal() {
    return false;  // This is a soft goal
  }

  // We'll implement optimize() in the next step
}
```

## Step 2: Implement the Optimize Method

The `optimize()` method is where your goal's logic lives:

```java
@Override
public OptimizationResult optimize(ClusterModel clusterModel,
                                   Set<Goal> optimizedGoals,
                                   OptimizationOptions options)
    throws OptimizationFailureException {

  // 1. Calculate current message count distribution
  Map<Broker, Double> messageCountByBroker = new HashMap<>();
  for (Broker broker : clusterModel.aliveBrokers()) {
    double totalMessages = 0.0;
    for (Replica replica : broker.replicas()) {
      // Assume we have a custom metric "messageRate"
      totalMessages += replica.load().expectedUtilizationFor(
          Resource.NW_IN) / AVG_MESSAGE_SIZE;
    }
    messageCountByBroker.put(broker, totalMessages);
  }

  // 2. Calculate average and identify imbalanced brokers
  double avgMessageCount = messageCountByBroker.values().stream()
      .mapToDouble(Double::doubleValue)
      .average()
      .orElse(0.0);

  List<Broker> overloadedBrokers = messageCountByBroker.entrySet().stream()
      .filter(e -> e.getValue() > avgMessageCount * balanceThreshold)
      .map(Map.Entry::getKey)
      .sorted(Comparator.comparing(messageCountByBroker::get).reversed())
      .collect(Collectors.toList());

  List<Broker> underloadedBrokers = messageCountByBroker.entrySet().stream()
      .filter(e -> e.getValue() < avgMessageCount / balanceThreshold)
      .map(Map.Entry::getKey)
      .sorted(Comparator.comparing(messageCountByBroker::get))
      .collect(Collectors.toList());

  // 3. Move partitions from overloaded to underloaded brokers
  int movementCount = 0;
  for (Broker overloaded : overloadedBrokers) {
    while (messageCountByBroker.get(overloaded) > avgMessageCount * balanceThreshold) {
      // Find best partition to move
      Replica replicaToMove = findBestReplicaToMove(overloaded, messageCountByBroker);
      if (replicaToMove == null) {
        break;  // No suitable partition found
      }

      // Find best destination broker
      Broker destination = findBestDestination(
          underloadedBrokers,
          replicaToMove,
          messageCountByBroker,
          avgMessageCount
      );
      if (destination == null) {
        break;  // No suitable destination
      }

      // Move the replica
      clusterModel.relocateReplica(
          replicaToMove.topicPartition(),
          overloaded.id(),
          destination.id()
      );

      // Update message counts
      double replicaMessages = replicaToMove.load().expectedUtilizationFor(
          Resource.NW_IN) / AVG_MESSAGE_SIZE;
      messageCountByBroker.put(overloaded,
          messageCountByBroker.get(overloaded) - replicaMessages);
      messageCountByBroker.put(destination,
          messageCountByBroker.get(destination) + replicaMessages);

      movementCount++;
    }
  }

  // 4. Return result
  return new OptimizationResult(
      clusterModel,
      name(),
      movementCount > 0 ? OptimizationResult.Status.OPTIMIZED
                        : OptimizationResult.Status.NO_ACTION_NEEDED
  );
}
```

## Step 3: Implement Helper Methods

```java
/**
 * Finds the best replica to move from the given broker.
 *
 * <p>Prefers followers over leaders (to avoid leadership transfer).
 * Selects largest replicas first for faster balance convergence.
 *
 * @param broker the broker to move from
 * @param messageCountByBroker current message counts
 * @return best replica to move, or null if none suitable
 */
private Replica findBestReplicaToMove(Broker broker,
                                     Map<Broker, Double> messageCountByBroker) {
  List<Replica> candidates = new ArrayList<>(broker.replicas());

  // Prefer followers over leaders
  candidates.sort(Comparator
      .comparing(Replica::isLeader)  // false < true, so followers first
      .thenComparing(r -> -r.load().expectedUtilizationFor(Resource.NW_IN))  // largest first
  );

  return candidates.isEmpty() ? null : candidates.get(0);
}

/**
 * Finds the best destination broker for the replica.
 *
 * @param candidates candidate destination brokers
 * @param replica the replica to move
 * @param messageCountByBroker current message counts
 * @param avgMessageCount average message count across all brokers
 * @return best destination broker, or null if none suitable
 */
private Broker findBestDestination(List<Broker> candidates,
                                  Replica replica,
                                  Map<Broker, Double> messageCountByBroker,
                                  double avgMessageCount) {
  double replicaMessages = replica.load().expectedUtilizationFor(
      Resource.NW_IN) / AVG_MESSAGE_SIZE;

  for (Broker candidate : candidates) {
    // Check if adding this replica would keep broker balanced
    double newMessageCount = messageCountByBroker.get(candidate) + replicaMessages;
    if (newMessageCount <= avgMessageCount * balanceThreshold) {
      return candidate;
    }
  }

  return null;  // No suitable destination
}
```

## Step 4: Implement Required Methods

```java
@Override
public ClusterModelStatsComparator clusterModelStatsComparator() {
  // Compare cluster models based on message count balance
  return new MessageCountBalanceComparator();
}

@Override
public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
  // This goal requires network metrics to calculate message rates
  return new ModelCompletenessRequirements(
      1,  // minMonitoredPartitionsPercentage
      1.0,  // minRequiredNumSnapshotWindows
      true  // includeAllTopics
  );
}

private static class MessageCountBalanceComparator implements ClusterModelStatsComparator {
  @Override
  public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
    // Lower variance in message distribution = better
    return Double.compare(
        calculateMessageCountVariance(stats1),
        calculateMessageCountVariance(stats2)
    );
  }

  private double calculateMessageCountVariance(ClusterModelStats stats) {
    // Calculate variance of message counts across brokers
    // (implementation omitted for brevity)
    return 0.0;
  }
}
```

## Step 5: Add Configuration

Add configuration properties to `cruisecontrol.properties`:

```properties
# Message Count Balance Goal configuration
message.count.balance.threshold=1.5
message.count.avg.message.size.bytes=1024
```

Load configuration in your goal:

```java
public MessageCountBalanceGoal(KafkaCruiseControlConfig config) {
  this.balanceThreshold = config.getDouble(
      "message.count.balance.threshold",
      DEFAULT_THRESHOLD
  );
  this.avgMessageSize = config.getLong(
      "message.count.avg.message.size.bytes",
      DEFAULT_AVG_MESSAGE_SIZE
  );
}
```

## Step 6: Register the Goal

### Option 1: Add to Default Goals

Edit `cruisecontrol.properties`:

```properties
# Default goals (in priority order)
goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,\
      com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,\
      com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,\
      ...
      com.example.cruisecontrol.goals.MessageCountBalanceGoal,\
      ...

# Hard goals (subset of goals)
hard.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,\
           com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal
```

### Option 2: Use at Request Time

Specify goals in API request:

```bash
curl -X POST "http://localhost:9090/kafkacruisecontrol/rebalance?\
goals=RackAwareGoal,ReplicaCapacityGoal,MessageCountBalanceGoal"
```

## Step 7: Write Tests

```java
package com.example.cruisecontrol.goals;

import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

public class MessageCountBalanceGoalTest {
  private MessageCountBalanceGoal goal;

  @BeforeEach
  public void setUp() {
    goal = new MessageCountBalanceGoal(1.5);
  }

  @Test
  public void testBalancedCluster() {
    // Arrange: Create already-balanced cluster
    ClusterModel clusterModel = createBalancedCluster();

    // Act: Optimize
    OptimizationResult result = goal.optimize(
        clusterModel,
        Collections.emptySet(),
        new OptimizationOptions()
    );

    // Assert: No changes needed
    assertThat(result.status()).isEqualTo(OptimizationResult.Status.NO_ACTION_NEEDED);
    assertThat(result.proposals()).isEmpty();
  }

  @Test
  public void testImbalancedCluster() {
    // Arrange: Create imbalanced cluster
    ClusterModel clusterModel = createImbalancedCluster();

    // Act: Optimize
    OptimizationResult result = goal.optimize(
        clusterModel,
        Collections.emptySet(),
        new OptimizationOptions()
    );

    // Assert: Partitions moved to balance
    assertThat(result.status()).isEqualTo(OptimizationResult.Status.OPTIMIZED);
    assertThat(result.proposals()).isNotEmpty();

    // Verify balance improved
    double varianceBefore = calculateMessageCountVariance(createImbalancedCluster());
    double varianceAfter = calculateMessageCountVariance(clusterModel);
    assertThat(varianceAfter).isLessThan(varianceBefore);
  }

  private ClusterModel createBalancedCluster() {
    // Create test cluster with balanced message distribution
    // (implementation omitted for brevity)
    return null;
  }

  private ClusterModel createImbalancedCluster() {
    // Create test cluster with imbalanced message distribution
    return null;
  }

  private double calculateMessageCountVariance(ClusterModel model) {
    // Calculate variance
    return 0.0;
  }
}
```

## Best Practices

### 1. Respect Previously Optimized Goals

Your goal should not violate goals that have already run:

```java
// Check if RackAwareGoal already optimized
if (optimizedGoals.contains(RackAwareGoal.class)) {
  // Don't move replicas in a way that violates rack awareness
  if (!isRackAware(sourceReplica, destinationBroker)) {
    continue;  // Skip this movement
  }
}
```

### 2. Use Efficient Algorithms

- Avoid O(n²) or worse algorithms
- Use sorted data structures for faster lookups
- Cache calculated values

### 3. Handle Edge Cases

- Empty clusters
- Single-broker clusters
- Brokers with no movable partitions
- Configuration with all brokers excluded

### 4. Log Important Decisions

```java
LOG.info("Moving partition {} from broker {} to broker {} to balance message count",
         replica.topicPartition(),
         source.id(),
         destination.id());
```

### 5. Provide Detailed Javadoc

See examples above for Javadoc format.

## Debugging Your Goal

### Enable Debug Logging

```properties
log4j.logger.com.example.cruisecontrol.goals=DEBUG
```

### Use Dry Run

Test your goal without actually executing:

```bash
curl -X POST "http://localhost:9090/kafkacruisecontrol/rebalance?\
goals=MessageCountBalanceGoal&dryrun=true&verbose=true"
```

### Visualize Proposals

```bash
# Get proposals in JSON
curl "http://localhost:9090/kafkacruisecontrol/proposals?\
goals=MessageCountBalanceGoal&json=true" | jq .
```

## Related Documentation

- [Architecture: GoalOptimizer](../architecture/02-goal-optimizer.md)
- [Contributing Guide](contributing.md)
- [Built-in Goals Reference](../../cruise-control-core/src/main/java/com/linkedin/kafka/cruisecontrol/analyzer/goals/)
