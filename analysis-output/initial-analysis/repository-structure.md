# Repository Structure - Cruise Control

**Analysis Commit:** `e30eaf352c31511241f4dfae457fbcb77022a9c6`

## Root Directory Layout

```
cruise-control/
├── .circleci/           # CircleCI CI/CD configuration
├── .github/
│   └── workflows/       # GitHub Actions workflows
│       ├── ci.yaml      # Main CI pipeline (JDK 17, multi-platform)
│       └── artifactory.yaml  # Artifact publishing
├── buildSrc/            # Gradle build logic
├── checkstyle/          # Checkstyle configuration
├── config/              # Runtime configuration files
├── cruise-control/      # Main Kafka implementation module (PRIMARY)
├── cruise-control-client/  # Python CLI client
├── cruise-control-core/    # Generic framework (platform-agnostic)
├── cruise-control-metrics-reporter/  # Kafka broker metrics plugin
├── docs/                # Documentation
├── fileStore/           # Runtime file storage
├── gradle/              # Gradle wrapper files
├── build.gradle         # Root build configuration
├── settings.gradle      # Gradle multi-project setup
├── kafka-cruise-control-start.sh   # Startup script
├── kafka-cruise-control-stop.sh    # Stop script
├── gradlew              # Gradle wrapper executable
├── LICENSE              # BSD 2-Clause License
├── README.md            # Project README
└── CONTRIBUTING.md      # Contribution guidelines
```

## Module Structure

### 1. cruise-control-core/
**Purpose:** Generic Cruise Control framework (could theoretically work with non-Kafka systems)

```
cruise-control-core/
└── src/
    ├── main/java/com/linkedin/cruisecontrol/
    │   ├── async/          # Async operation support
    │   ├── common/         # Common utilities
    │   ├── config/         # Core configuration
    │   ├── detector/       # Generic anomaly detection
    │   ├── exception/      # Exception hierarchy
    │   ├── http/           # HTTP utilities
    │   ├── metricdef/      # Metric definitions
    │   ├── model/          # Core domain model
    │   ├── monitor/        # Generic monitoring
    │   │   ├── sampling/   # Metric sampling abstractions
    │   │   └── task/       # Monitoring tasks
    │   └── servlet/        # Servlet abstractions
    └── test/java/          # Unit tests (mirrors main structure)
```

**Key Files:**
- `CruiseControlConfigurable.java` - Interface for pluggable components
- `MonitoredResource.java` - Resource type enumeration
- `MetricDef.java` - Metric definition framework

### 2. cruise-control-metrics-reporter/
**Purpose:** Kafka broker plugin that collects and reports metrics

```
cruise-control-metrics-reporter/
└── src/
    ├── main/java/com/linkedin/kafka/cruisecontrol/metricsreporter/
    │   ├── CruiseControlMetricsReporter.java  # Main metrics reporter (runs on broker)
    │   ├── CruiseControlMetricsReporterConfig.java
    │   ├── CruiseControlMetricsUtils.java
    │   ├── metric/         # Metric collection logic
    │   │   ├── BrokerMetric.java
    │   │   ├── CruiseControlMetric.java
    │   │   ├── MetricSerde.java
    │   │   ├── RawMetricType.java
    │   │   └── TopicMetric.java
    │   └── exception/      # Reporter-specific exceptions
    └── test/java/          # Unit tests
```

**Key Concept:** This JAR is deployed to Kafka brokers, not Cruise Control server.

### 3. cruise-control/ (MAIN MODULE)
**Purpose:** Kafka-specific implementation of Cruise Control

```
cruise-control/
└── src/
    ├── main/
    │   ├── java/com/linkedin/kafka/cruisecontrol/
    │   │   ├── analyzer/         # ⭐ Optimization engine
    │   │   │   ├── goals/        # 28 optimization goals
    │   │   │   ├── kafkaassigner/ # Legacy Kafka assigner goals
    │   │   │   ├── ActionAcceptance.java
    │   │   │   ├── ActionType.java
    │   │   │   ├── AnalyzerState.java
    │   │   │   ├── AnalyzerUtils.java
    │   │   │   ├── BalancingAction.java
    │   │   │   ├── BalancingConstraint.java
    │   │   │   ├── GoalOptimizer.java  # 33KB - main optimizer
    │   │   │   ├── OptimizationOptions.java
    │   │   │   └── OptimizerResult.java
    │   │   │
    │   │   ├── async/            # Async operation framework
    │   │   │   ├── AsyncKafkaCruiseControl.java
    │   │   │   ├── OperationFuture.java
    │   │   │   └── progress/     # Progress tracking
    │   │   │
    │   │   ├── common/           # Common utilities
    │   │   │   ├── Resource.java # CPU/DISK/NW_IN/NW_OUT enum
    │   │   │   ├── KafkaCruiseControlThreadFactory.java
    │   │   │   ├── MetadataClient.java
    │   │   │   └── Utils.java
    │   │   │
    │   │   ├── config/           # ⭐ Configuration system
    │   │   │   ├── constants/    # Config constant classes (8 files)
    │   │   │   │   ├── AnalyzerConfig.java       # 45KB
    │   │   │   │   ├── ExecutorConfig.java       # 52KB
    │   │   │   │   ├── MonitorConfig.java        # 35KB
    │   │   │   │   ├── AnomalyDetectorConfig.java # 27KB
    │   │   │   │   ├── WebServerConfig.java      # 33KB
    │   │   │   │   └── ... (3 more)
    │   │   │   ├── BrokerCapacityInfo.java
    │   │   │   ├── BrokerCapacityConfigFileResolver.java
    │   │   │   ├── KafkaCruiseControlConfig.java # 35KB - main config
    │   │   │   ├── KafkaAdminTopicConfigProvider.java
    │   │   │   └── TopicConfigProvider.java
    │   │   │
    │   │   ├── detector/         # ⭐ Anomaly detection
    │   │   │   ├── notifier/     # 4 notifier implementations
    │   │   │   │   ├── SelfHealingNotifier.java
    │   │   │   │   ├── SlackSelfHealingNotifier.java
    │   │   │   │   ├── MSTeamsSelfHealingNotifier.java
    │   │   │   │   └── AlertaSelfHealingNotifier.java
    │   │   │   ├── AnomalyDetectorManager.java  # 32KB - main detector
    │   │   │   ├── AnomalyDetectorState.java
    │   │   │   ├── BrokerFailures.java
    │   │   │   ├── DiskFailures.java
    │   │   │   ├── GoalViolations.java
    │   │   │   ├── KafkaBrokerFailureDetector.java
    │   │   │   ├── DiskFailureDetector.java
    │   │   │   ├── GoalViolationDetector.java
    │   │   │   ├── MetricAnomalyDetector.java
    │   │   │   ├── TopicAnomalyDetector.java
    │   │   │   ├── MaintenanceEventDetector.java
    │   │   │   ├── SlowBrokerFinder.java
    │   │   │   └── ... (anomaly types and handlers)
    │   │   │
    │   │   ├── executor/         # ⭐ Execution engine
    │   │   │   ├── strategy/     # 9 execution strategies
    │   │   │   │   ├── ReplicaMovementStrategy.java (interface)
    │   │   │   │   ├── BaseReplicaMovementStrategy.java
    │   │   │   │   ├── PostponeUrpReplicaMovementStrategy.java
    │   │   │   │   ├── PrioritizeLargeReplicaMovementStrategy.java
    │   │   │   │   └── ... (5 more strategies)
    │   │   │   ├── Executor.java            # 130KB - LARGEST FILE
    │   │   │   ├── ExecutionProposal.java
    │   │   │   ├── ExecutionTask.java
    │   │   │   ├── ExecutionTaskManager.java
    │   │   │   ├── ExecutionTaskPlanner.java
    │   │   │   ├── ExecutionTaskTracker.java
    │   │   │   ├── ExecutorNotifier.java
    │   │   │   ├── ExecutorState.java
    │   │   │   ├── ReplicationThrottleHelper.java
    │   │   │   └── ... (execution support classes)
    │   │   │
    │   │   ├── model/            # ⭐ Domain model
    │   │   │   ├── ClusterModel.java      # 63KB - Central model
    │   │   │   ├── Broker.java            # 23KB
    │   │   │   ├── Replica.java           # 14KB
    │   │   │   ├── Partition.java         # 8KB
    │   │   │   ├── Disk.java              # 8KB (JBOD support)
    │   │   │   ├── Rack.java              # 11KB
    │   │   │   ├── Host.java              # 8KB
    │   │   │   ├── Load.java              # 11KB
    │   │   │   ├── ClusterModelStats.java # 25KB
    │   │   │   ├── SortedReplicas.java
    │   │   │   ├── ReplicaSortFunctionFactory.java
    │   │   │   └── ... (model utilities)
    │   │   │
    │   │   ├── monitor/          # ⭐ Monitoring and metrics
    │   │   │   ├── sampling/     # Metric sampling
    │   │   │   │   ├── MetricSampler.java (interface)
    │   │   │   │   ├── CruiseControlMetricsReporterSampler.java
    │   │   │   │   ├── PrometheusMetricSampler.java
    │   │   │   │   ├── SampleStore.java (interface)
    │   │   │   │   ├── KafkaSampleStore.java
    │   │   │   │   └── ... (sampling support)
    │   │   │   ├── task/         # Monitoring tasks
    │   │   │   ├── metricdefinition/
    │   │   │   │   └── KafkaMetricDef.java
    │   │   │   ├── LoadMonitor.java       # 43KB - Main monitor
    │   │   │   ├── LoadMonitorState.java
    │   │   │   ├── LoadMonitorTaskRunner.java
    │   │   │   ├── MetricFetcherManager.java
    │   │   │   ├── ModelGeneration.java
    │   │   │   └── ... (monitor support)
    │   │   │
    │   │   ├── servlet/          # ⭐ REST API layer
    │   │   │   ├── handler/      # Request handlers
    │   │   │   │   ├── async/    # Async handlers
    │   │   │   │   │   └── runnable/  # 15 runnable classes
    │   │   │   │   │       ├── AddBrokersRunnable.java
    │   │   │   │   │       ├── RemoveBrokersRunnable.java
    │   │   │   │   │       ├── RebalanceRunnable.java
    │   │   │   │   │       ├── ProposalsRunnable.java
    │   │   │   │   │       └── ... (11 more)
    │   │   │   │   └── sync/     # Sync handlers
    │   │   │   │       ├── BootstrapHandler.java
    │   │   │   │       ├── PauseHandler.java
    │   │   │   │       ├── ResumeHandler.java
    │   │   │   │       └── ... (5 more)
    │   │   │   ├── parameters/   # 33 parameter classes
    │   │   │   ├── response/     # 24 response classes
    │   │   │   ├── security/     # Security providers
    │   │   │   │   ├── SecurityProvider.java
    │   │   │   │   ├── BasicSecurityProvider.java
    │   │   │   │   ├── JwtSecurityProvider.java
    │   │   │   │   ├── SpnegoSecurityProvider.java
    │   │   │   │   └── TrustedProxySecurityProvider.java
    │   │   │   ├── CruiseControlEndPoint.java  # Endpoint enum
    │   │   │   ├── UserTaskManager.java   # 37KB - Task tracking
    │   │   │   ├── KafkaCruiseControlServlet.java
    │   │   │   └── ... (servlet support)
    │   │   │
    │   │   ├── vertx/            # Vert.x web framework integration
    │   │   │   ├── MainVerticle.java
    │   │   │   └── VertxUtils.java
    │   │   │
    │   │   ├── KafkaCruiseControl.java     # 78KB - Main facade
    │   │   ├── KafkaCruiseControlMain.java # Entry point
    │   │   ├── KafkaCruiseControlApp.java
    │   │   ├── KafkaCruiseControlServletApp.java  # Jetty version
    │   │   ├── KafkaCruiseControlVertxApp.java    # Vert.x version
    │   │   └── KafkaCruiseControlUtils.java
    │   │
    │   ├── scala/com/linkedin/kafka/cruisecontrol/
    │   │   └── (Scala source files for Kafka integration)
    │   │
    │   └── resources/
    │       ├── yaml/             # OpenAPI specifications
    │       │   └── base.yaml
    │       ├── webroot/          # Web UI resources
    │       └── (other resources)
    │
    ├── test/
    │   ├── java/com/linkedin/kafka/cruisecontrol/
    │   │   ├── analyzer/
    │   │   │   ├── goals/        # Goal-specific tests
    │   │   │   └── ... (analyzer tests)
    │   │   ├── common/
    │   │   │   ├── DeterministicCluster.java  # 107KB test utility
    │   │   │   ├── TestConstants.java
    │   │   │   └── ... (test utilities)
    │   │   ├── detector/         # Anomaly detection tests
    │   │   ├── executor/         # Executor tests
    │   │   ├── model/            # Model tests
    │   │   ├── monitor/          # Monitor tests
    │   │   ├── servlet/          # REST API tests
    │   │   └── ... (other test packages)
    │   │
    │   ├── scala/                # Scala tests
    │   └── resources/            # Test resources
    │       └── log4j2.properties
    │
    └── integrationTest/
        └── java/                 # Integration tests (5 test files)
            ├── BrokerFailureIntegrationTest.java
            ├── DiskFailureIntegrationTest.java
            └── ... (3 more)
```

### 4. cruise-control-client/
**Purpose:** Python CLI client for Cruise Control

```
cruise-control-client/
└── cruisecontrolclient/
    ├── client/           # Client implementation
    ├── util/             # Utilities
    └── cruisecontrol.py  # Main CLI entry point
```

## Configuration Files

```
config/
├── cruisecontrol.properties  # Main configuration (349 lines)
├── capacity.json             # Broker capacity definitions
├── capacityJBOD.json         # JBOD capacity definitions
├── clusterConfigs.json       # Cluster-specific configs
└── log4j.properties          # Logging configuration
```

## Key File Size Analysis

| File | Lines | Purpose |
|------|-------|---------|
| Executor.java | ~3,500 | Execution engine - handles all rebalancing |
| ClusterModel.java | ~2,200 | Central domain model |
| KafkaCruiseControl.java | ~2,100 | Main facade coordinating all components |
| LoadMonitor.java | ~1,500 | Metrics collection and cluster model building |
| UserTaskManager.java | ~1,300 | Async operation tracking |
| GoalOptimizer.java | ~1,200 | Optimization proposal generation |
| DeterministicCluster.java | ~3,000 | Test cluster generation utility |
| AnomalyDetectorManager.java | ~1,100 | Anomaly detection orchestration |
| KafkaCruiseControlConfig.java | ~1,200 | Configuration management |
| Broker.java | ~800 | Broker model |

## Package Organization Insights

### Horizontal Slicing (by layer)
- `config/` - Configuration layer
- `servlet/` - Presentation layer (REST API)
- `monitor/`, `analyzer/`, `executor/`, `detector/` - Business logic layers

### Vertical Slicing (by feature)
- Each subsystem is self-contained: monitor, analyzer, executor, detector
- Pluggable components organized under respective packages

### Design Pattern Organization
- `analyzer/goals/` - 28 Goal implementations (Strategy pattern)
- `executor/strategy/` - 9 Execution strategies (Strategy pattern)
- `servlet/handler/async/runnable/` - 15 Async handlers (Command pattern)
- `servlet/security/` - 5 Security providers (Strategy pattern)
- `detector/notifier/` - 4 Notifiers (Observer pattern)

## Build System Structure

```
build.gradle
├── Root project
├── Subproject: cruise-control-core
├── Subproject: cruise-control-metrics-reporter
└── Subproject: cruise-control

Each subproject has:
- Java/Scala compilation
- Checkstyle (code style)
- SpotBugs (static analysis)
- JaCoCo (code coverage)
- JUnit (testing)
```

## CI/CD Structure

```
.github/workflows/
├── ci.yaml
│   ├── Test job (JDK 17, Microsoft & Temurin)
│   ├── Integration test job
│   └── Platform build (s390x, ppc64le via QEMU)
└── artifactory.yaml
    └── Publish to JFrog Artifactory
```

## Documentation Structure

```
docs/
├── wiki/             # Wiki documentation
└── images/           # Diagrams and screenshots
```

## Navigation Tips

**Want to understand...**
- **How it starts?** → `KafkaCruiseControlMain.java:26`
- **Main coordination?** → `KafkaCruiseControl.java:78`
- **REST API?** → `servlet/CruiseControlEndPoint.java:16`
- **Monitoring?** → `monitor/LoadMonitor.java:176`
- **Optimization?** → `analyzer/GoalOptimizer.java:435`
- **Execution?** → `executor/Executor.java:810`
- **Anomaly detection?** → `detector/AnomalyDetectorManager.java:112`
- **Configuration?** → `config/constants/` (8 config classes)
- **A specific goal?** → `analyzer/goals/RackAwareGoal.java`
- **Security?** → `servlet/security/SecurityProvider.java`
- **Testing patterns?** → `test/java/.../common/DeterministicCluster.java`

---

**Total:** 652 Java files, ~68,000 lines of main code, ~25,000 lines of test code
