# Contributing to Cruise Control

Thank you for your interest in contributing to Cruise Control! This guide will help you get started.

## Quick Start

### 1. Fork and Clone

```bash
# Fork on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/cruise-control.git
cd cruise-control

# Add upstream remote
git remote add upstream https://github.com/linkedin/cruise-control.git
```

### 2. Build

```bash
# Build the project
./gradlew build

# This will:
# - Compile all source code
# - Run unit tests
# - Run integration tests
# - Generate Javadoc
# - Run Checkstyle validation
```

### 3. Run Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests ExecutorTest

# Run tests with coverage
./gradlew test jacocoTestReport
# Report: build/reports/jacoco/test/html/index.html
```

### 4. Run Locally

```bash
# Start Kafka and Zookeeper (if not running)
cd kafka_2.13-3.0.0
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

# Start Cruise Control
./gradlew jar
./kafka-cruise-control-start.sh config/cruisecontrol.properties

# Access UI
open http://localhost:9090
```

## Development Workflow

### 1. Create a Feature Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/my-new-feature
```

### 2. Make Changes

- Write code following our [Code Style](#code-style)
- Add/update tests for your changes
- Add/update documentation (Javadoc, user docs)
- Run tests locally before committing

### 3. Commit Your Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "Add support for custom goal configuration

- Implement ConfigurableGoal interface
- Add configuration validation
- Update documentation and examples
"
```

**Commit Message Guidelines:**
- First line: Short summary (50-72 characters)
- Use present tense ("Add feature" not "Added feature")
- Include context in the body if needed
- Reference issues: "Fixes #123" or "Related to #456"

### 4. Push and Create Pull Request

```bash
# Push to your fork
git push origin feature/my-new-feature

# Create PR on GitHub
# Fill in the PR template with:
# - Description of changes
# - Motivation and context
# - Testing performed
# - Related issues
```

## Code Style

### Java Code Style

Cruise Control follows Google Java Style Guide with minor modifications.

**Key Points:**
- **Indentation:** 2 spaces (not tabs)
- **Line length:** 120 characters max
- **Imports:** No wildcards, organized alphabetically
- **Braces:** Required for all if/for/while blocks
- **Naming:**
  - Classes: `PascalCase`
  - Methods/variables: `camelCase`
  - Constants: `UPPER_SNAKE_CASE`

**Example:**
```java
public class MyCustomGoal extends AbstractGoal {
  private static final int DEFAULT_PRIORITY = 10;
  private final GoalOptions options;

  public MyCustomGoal(GoalOptions options) {
    this.options = options;
  }

  @Override
  public OptimizationResult optimize(ClusterModel clusterModel,
                                     Set<Goal> optimizedGoals,
                                     OptimizationOptions options) {
    // Implementation
  }
}
```

### Run Checkstyle

```bash
# Check style violations
./gradlew checkstyleMain checkstyleTest

# Reports: build/reports/checkstyle/
```

**Fix common violations automatically:**
```bash
# Use spotless (if configured)
./gradlew spotlessApply
```

## Documentation Standards

### Javadoc Requirements

All public classes and methods MUST have Javadoc.

**Class Documentation:**
```java
/**
 * Balances partitions based on custom metric X.
 *
 * <p>This goal ensures that all brokers have similar values for metric X,
 * which represents [explanation]. It achieves this by moving partitions
 * from brokers with high X values to brokers with low X values.
 *
 * <h3>Priority</h3>
 * <p>This is a soft goal with default priority 10.
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code custom.metric.x.threshold} - Maximum allowed deviation</li>
 * </ul>
 *
 * <h3>Performance</h3>
 * <p>Time Complexity: O(brokers × partitions)
 * <p>Typical execution: 10-30 seconds for 1000 brokers
 *
 * @see AbstractGoal
 * @see GoalOptimizer
 */
public class CustomMetricBalanceGoal extends AbstractGoal {
  // ...
}
```

**Method Documentation:**
```java
/**
 * Optimizes the cluster according to this goal.
 *
 * <p>This method iterates through all brokers and identifies partitions
 * that can be moved to improve balance. It modifies the ClusterModel
 * in-place.
 *
 * <h4>Algorithm</h4>
 * <ol>
 *   <li>Calculate current metric distribution</li>
 *   <li>Identify over-utilized and under-utilized brokers</li>
 *   <li>Generate partition movements to balance metric</li>
 *   <li>Validate movements don't violate other constraints</li>
 * </ol>
 *
 * @param clusterModel the cluster state to optimize (modified in-place)
 * @param optimizedGoals goals that have already optimized the model
 * @param options optimization options (e.g., excluded brokers)
 * @return optimization result with proposals and statistics
 * @throws OptimizationFailureException if goal cannot be satisfied
 */
@Override
public OptimizationResult optimize(ClusterModel clusterModel,
                                   Set<Goal> optimizedGoals,
                                   OptimizationOptions options)
    throws OptimizationFailureException {
  // Implementation
}
```

### Inline Comments

Use inline comments for:
- Complex algorithms
- Non-obvious design decisions
- Performance optimizations
- Workarounds for external issues

**Example:**
```java
// Sort brokers by load descending to prioritize moving from high-load brokers
// This improves convergence speed by ~30% compared to random ordering
List<Broker> sortedBrokers = new ArrayList<>(clusterModel.brokers());
sortedBrokers.sort(Comparator.comparing(Broker::load).reversed());

for (Broker broker : sortedBrokers) {
  // Skip brokers that are already under threshold
  if (broker.load() < threshold) {
    continue;
  }

  // Find partitions to move
  // Note: We prefer moving follower replicas over leader replicas
  // because follower moves don't require leadership transfer
  List<Replica> followers = broker.replicas().stream()
      .filter(r -> !r.isLeader())
      .sorted(Comparator.comparing(Replica::load).reversed())
      .collect(Collectors.toList());

  // ... movement logic ...
}
```

## Testing Guidelines

### Test Structure

```java
public class MyCustomGoalTest {
  private ClusterModel clusterModel;
  private MyCustomGoal goal;

  @BeforeEach
  public void setUp() {
    // Create test cluster with known state
    clusterModel = DeterministicCluster.getHomogeneousDiskDistribution(
        /* numBrokers */ 3,
        /* numRacks */ 3,
        /* numTopics */ 10,
        /* numPartitions */ 100,
        /* replicationFactor */ 2
    );

    goal = new MyCustomGoal();
  }

  @Test
  public void testBasicOptimization() {
    // Arrange: Create imbalanced cluster
    // ... setup code ...

    // Act: Optimize
    OptimizationResult result = goal.optimize(
        clusterModel,
        Collections.emptySet(),
        new OptimizationOptions()
    );

    // Assert: Verify balance improved
    assertThat(result.goalViolations()).isEmpty();
    assertThat(result.proposalCount()).isGreaterThan(0);
    // ... more assertions ...
  }

  @Test
  public void testGoalViolation() {
    // Test that goal detects violations correctly
  }

  @Test
  public void testWithExcludedBrokers() {
    // Test optimization with broker exclusions
  }
}
```

### Test Coverage

- **Minimum:** 80% line coverage for new code
- **Ideal:** 90%+ line coverage
- **Required:** All public API methods tested

**Check coverage:**
```bash
./gradlew test jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

### Integration Tests

For features that interact with Kafka:

```java
@IntegrationTest
public class ExecutorIntegrationTest {
  private EmbeddedKafkaCluster kafkaCluster;
  private Executor executor;

  @BeforeEach
  public void setUp() {
    kafkaCluster = new EmbeddedKafkaCluster(3);  // 3 brokers
    kafkaCluster.start();

    executor = new Executor(kafkaCluster.bootstrapServers());
  }

  @Test
  public void testPartitionReassignment() {
    // Create topic
    kafkaCluster.createTopic("test-topic", 10, (short) 2);

    // Generate proposals
    List<ExecutionProposal> proposals = createTestProposals();

    // Execute
    executor.executeProposals(proposals);

    // Verify partitions moved
    // ...
  }

  @AfterEach
  public void tearDown() {
    kafkaCluster.stop();
  }
}
```

## Pull Request Process

### 1. Before Submitting

- [ ] All tests pass locally
- [ ] Checkstyle passes
- [ ] Documentation updated (Javadoc, user docs, README)
- [ ] Commit messages are clear and descriptive
- [ ] PR description filled out completely

### 2. PR Review

- Maintainers will review within 1-2 weeks
- Address feedback by pushing new commits
- Once approved, squash commits if requested
- Maintainer will merge when ready

### 3. After Merge

- Delete your feature branch
- Sync your fork with upstream
- Celebrate! Thank you for contributing!

## Common Tasks

### Adding a New Goal

See [Custom Goals Guide](custom-goals.md) for detailed instructions.

**Quick checklist:**
1. Create class extending `AbstractGoal`
2. Implement required methods (`optimize`, `name`, etc.)
3. Add configuration properties if needed
4. Write unit tests
5. Add documentation (Javadoc + user guide)
6. Update default goals list (if applicable)

### Adding a New Anomaly Detector

See [Custom Anomaly Detectors Guide](custom-anomaly-detectors.md).

### Fixing a Bug

1. Create test that reproduces the bug
2. Fix the bug
3. Verify test now passes
4. Check for similar issues elsewhere
5. Submit PR with test + fix

## Getting Help

- **Slack:** Join [Cruise Control Slack](https://linkedin-cruise-control.slack.com)
- **Mailing List:** cruise-control-users@googlegroups.com
- **Issues:** https://github.com/linkedin/cruise-control/issues
- **Documentation:** https://github.com/linkedin/cruise-control/wiki

## Code of Conduct

Be respectful, inclusive, and professional. We're all here to make Cruise Control better!

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
