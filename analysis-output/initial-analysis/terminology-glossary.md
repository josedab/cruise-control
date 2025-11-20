# Terminology Glossary - Cruise Control

**Analysis Commit:** `e30eaf352c31511241f4dfae457fbcb77022a9c6`

A comprehensive guide to Cruise Control-specific terminology and concepts.

## Core Concepts

### Cruise Control Components

**Cruise Control**
- The overall system for managing and optimizing Apache Kafka clusters at scale
- Developed by LinkedIn to handle their 10K+ broker deployments

**CruiseControlMetricsReporter**
- Kafka broker plugin (JAR deployed to each broker)
- Collects broker and partition metrics
- Sends metrics to `__CruiseControlMetrics` Kafka topic
- Location: `cruise-control-metrics-reporter/` module

**ClusterModel**
- In-memory representation of the Kafka cluster state
- Contains all brokers, racks, replicas, partitions, and their loads
- Built from aggregated metrics
- Core class: `com.linkedin.kafka.cruisecontrol.model.ClusterModel`

**LoadMonitor**
- Component that continuously collects metrics from Kafka brokers
- Builds and maintains the ClusterModel
- Manages metric sampling and aggregation
- Core class: `com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor`

**GoalOptimizer**
- Component that generates optimization proposals
- Runs goals in priority order to optimize the cluster
- Produces `ExecutionProposal` sets
- Core class: `com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer`

**Executor**
- Component that executes approved optimization proposals
- Safely moves partitions and changes leaders
- Manages concurrency, throttling, and rollback
- Core class: `com.linkedin.kafka.cruisecontrol.executor.Executor`

**AnomalyDetectorManager**
- Orchestrates multiple anomaly detectors
- Coordinates self-healing actions
- Manages anomaly priority queue
- Core class: `com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager`

## Goals & Optimization

**Goal**
- A pluggable optimization objective for the cluster
- Examples: RackAwareGoal, DiskCapacityGoal, CpuUsageDistributionGoal
- Interface: `com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal`
- Total: 28 built-in goals

**Hard Goals**
- Goals that MUST be satisfied (non-negotiable constraints)
- Example: RackAwareGoal (rack awareness for availability)
- Configured via `hard.goals` property
- Violations prevent proposal acceptance

**Default Goals**
- Goals used for pre-computed proposals and general optimization
- Must be a subset of all `goals` and superset of `hard.goals`
- Configured via `default.goals` property

**Self-Healing Goals**
- Goals used specifically for anomaly self-healing
- Typically more conservative than default goals
- Configured via `anomaly.detection.goals` property

**Intra-Broker Goals**
- Goals that optimize disk usage within a single broker (JBOD)
- Example: IntraBrokerDiskCapacityGoal
- Only apply to multi-disk broker configurations

**Goal Violation**
- State where current cluster configuration doesn't satisfy a goal
- Detected by GoalViolationDetector
- Can trigger self-healing if enabled

**Goal Priority**
- Order in which goals are executed during optimization
- Higher priority goals execute first
- Later goals must preserve earlier goals' optimizations

**Balancedness Score**
- Metric indicating how well a cluster satisfies goals
- Considers goal priority weights and strictness
- Used to compare optimization proposals

## Cluster Model Components

**Broker**
- Representation of a Kafka broker in the ClusterModel
- Contains replicas, capacity info, current load
- States: ALIVE, DEAD, NEW
- Class: `com.linkedin.kafka.cruisecontrol.model.Broker`

**Replica**
- Representation of a partition replica
- Can be LEADER or FOLLOWER
- Belongs to a specific broker and disk
- Class: `com.linkedin.kafka.cruisecontrol.model.Replica`

**Partition**
- Representation of a Kafka topic partition
- Contains list of all replicas (including leader)
- Class: `com.linkedin.kafka.cruisecontrol.model.Partition`

**Rack**
- Representation of a physical rack (failure domain)
- Contains multiple brokers
- Used for rack-aware replica placement
- Class: `com.linkedin.kafka.cruisecontrol.model.Rack`

**Host**
- Representation of a physical/virtual machine
- Can host multiple brokers (rare, usually 1:1)
- Class: `com.linkedin.kafka.cruisecontrol.model.Host`

**Disk**
- Representation of a disk/logDir on a broker
- JBOD support: broker can have multiple disks
- Class: `com.linkedin.kafka.cruisecontrol.model.Disk`

**Load**
- Resource utilization metrics (CPU, DISK, NW_IN, NW_OUT)
- Tracked per replica, broker, rack, cluster
- Class: `com.linkedin.kafka.cruisecontrol.model.Load`

## Metrics & Monitoring

**Metric Sample**
- A point-in-time measurement of resource utilization
- Collected at configured intervals (default: 120 seconds)
- Types: Broker metrics, Partition metrics

**Metric Window**
- Time period for aggregating metric samples
- Default: 300 seconds (5 minutes)
- Multiple windows kept in memory for historical analysis

**Partition Metrics Window**
- Configured via `partition.metrics.window.ms` (default: 300000)
- Number of windows: `num.partition.metrics.windows` (default: 5)
- Total history: 5 windows × 5 min = 25 minutes

**Broker Metrics Window**
- Configured via `broker.metrics.window.ms` (default: 300000)
- Number of windows: `num.broker.metrics.windows` (default: 20)
- Total history: 20 windows × 5 min = 100 minutes (1.67 hours)

**Metric Sampler**
- Pluggable component that collects metrics from brokers
- Interface: `com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler`
- Default: CruiseControlMetricsReporterSampler

**Sample Store**
- Pluggable storage for metric samples
- Interface: `com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore`
- Default: KafkaSampleStore (stores in Kafka topics)

**Model Completeness**
- Percentage of partitions with sufficient metric data
- Required for generating valid optimization proposals
- Configured via `min.valid.partition.ratio` (default: 0.95 = 95%)

**Monitored Partitions Ratio**
- Ratio of partitions with valid metrics vs. total partitions
- Must meet threshold before proposals can be generated

## Resources

**Resource (enum)**
- Type of resource being tracked: CPU, DISK, NW_IN, NW_OUT
- Each resource has capacity and utilization
- Enum: `com.linkedin.kafka.cruisecontrol.common.Resource`

**Capacity**
- Maximum resource limit for a broker
- Defined in capacity.json configuration file
- Example: CPU cores, disk GB, network MB/s

**Utilization**
- Current resource usage level
- Measured via metrics from brokers
- Expressed as rate (MB/s) or percentage

**Balance Threshold**
- Acceptable variance in resource distribution across brokers
- Example: `cpu.balance.threshold=1.1` (10% variance)
- Used by distribution goals

**Capacity Threshold**
- Maximum allowed resource utilization per broker
- Example: `disk.capacity.threshold=0.8` (80% max)
- Used by capacity goals

## Execution Concepts

**Execution Proposal**
- A specific action to move a replica or change leadership
- Types: Inter-broker move, Intra-broker move, Leadership change
- Class: `com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal`

**Inter-Broker Replica Movement**
- Moving a replica from one broker to another broker
- Most expensive operation (network transfer)
- Concurrency: `num.concurrent.partition.movements.per.broker`

**Intra-Broker Replica Movement**
- Moving a replica between disks on the same broker
- JBOD support only
- Concurrency: `num.concurrent.intra.broker.partition.movements`

**Leadership Movement**
- Changing which replica is the partition leader
- Fastest operation (no data transfer)
- Concurrency: `num.concurrent.leader.movements`

**Replication Throttle**
- Bandwidth limit for replica movements
- Prevents cluster overload during rebalancing
- Configured per-broker, per-topic via Kafka dynamic configs

**Execution Task**
- A single unit of work in execution (one proposal)
- States: PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED
- Class: `com.linkedin.kafka.cruisecontrol.executor.ExecutionTask`

**Execution State**
- Current phase of the execution engine
- Values: NO_TASK_IN_PROGRESS, GENERATING_PROPOSALS_FOR_EXECUTION,
  INITIALIZING_PROPOSAL_EXECUTION, STARTING_EXECUTION,
  INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, etc.

**URP (Under-Replicated Partition)**
- Partition with fewer in-sync replicas than replication factor
- Considered unhealthy, prioritized differently in execution strategies

**MinISR (Minimum In-Sync Replicas)**
- Kafka configuration for partition availability
- Execution strategies may adjust concurrency based on ISR status

## Anomaly Detection

**Anomaly**
- Unexpected cluster state requiring attention
- Types: Broker failure, Disk failure, Goal violation, Metric anomaly, etc.
- Can trigger self-healing if enabled

**Anomaly Detector**
- Component that monitors for specific anomaly type
- Base: `com.linkedin.kafka.cruisecontrol.detector.AbstractAnomalyDetector`
- 7 built-in detectors

**Broker Failure Detector**
- Detects when Kafka brokers go offline
- Uses Kafka metadata to identify dead brokers
- Can auto-trigger replica redistribution

**Disk Failure Detector**
- Detects when broker disks fail (JBOD)
- Uses Kafka log directory information
- Can auto-trigger replica redistribution from failed disk

**Goal Violation Detector**
- Periodically checks if cluster satisfies goals
- Runs goal optimizer to detect violations
- Can auto-trigger rebalancing

**Metric Anomaly Detector**
- Detects unusual metric patterns (e.g., high latency)
- Uses percentile-based thresholds
- Configured metrics: `metric.anomaly.analyzer.metrics`

**Slow Broker Finder**
- Detects brokers performing slower than peers
- Uses metric comparisons across brokers

**Topic Anomaly Detector**
- Detects topic-level issues (e.g., replication factor anomalies)
- Can detect partition size anomalies

**Maintenance Event Detector**
- Handles planned maintenance events
- Allows proactive rebalancing before maintenance

**Anomaly Notifier**
- Pluggable component that handles anomaly notifications
- Interface: `com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier`
- Implementations: SelfHealingNotifier, SlackSelfHealingNotifier, etc.

**Self-Healing**
- Automatic remediation of detected anomalies
- Configurable per anomaly type
- Default: disabled (`self.healing.enabled=false`)

**Anomaly Priority**
- Order in which anomalies are processed
- Higher priority anomalies handled first
- Example: Broker failures > Goal violations

## User Operations

**User Task**
- An async operation initiated via REST API
- Tracked with UUID for progress monitoring
- Class: `com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager.UserTaskInfo`

**Rebalance**
- Full cluster optimization based on goals
- Endpoint: POST `/rebalance`
- Generates and executes proposals

**Add Broker**
- Operation to add new broker(s) to cluster
- Endpoint: POST `/add_broker`
- Redistributes load to new brokers

**Remove Broker**
- Operation to remove broker(s) from cluster
- Endpoint: POST `/remove_broker`
- Moves replicas off target brokers

**Demote Broker**
- Makes broker give up leadership roles
- Endpoint: POST `/demote_broker`
- Useful before maintenance or decommission

**Fix Offline Replicas**
- Attempts to recover offline replicas
- Endpoint: POST `/fix_offline_replicas`
- May reassign partitions if needed

**Preferred Leader Election (PLE)**
- Makes first replica in replica list the leader
- Endpoint: POST `/admin?preferred_leader_election`
- Restores balanced leadership

**Topic Configuration Anomaly**
- Detects topics with non-standard configurations
- Example: Incorrect replication factor

## Configuration Concepts

**Broker Capacity**
- Resource limits for each broker
- Stored in capacity.json file
- Can be heterogeneous (different brokers, different capacities)

**JBOD (Just a Bunch Of Disks)**
- Configuration where broker has multiple independent disks
- Each disk tracked separately in ClusterModel
- Requires intra-broker goals for optimization

**Heterogeneous Cluster**
- Cluster with brokers of different capacities
- Requires broker-specific capacity configuration
- Common in cloud environments

**Kafka Assigner**
- Legacy Kafka partition assignment tool
- Cruise Control has compatible goals (KafkaAssignerEvenRackAwareGoal)
- Migration path from kafka-reassign-partitions.sh

**Two-Step Verification**
- Optional safety feature for POST requests
- Requires confirmation before executing destructive operations
- Configured via `two.step.verification.enabled`

## Advanced Concepts

**Provisioning**
- Recommendations for adding/removing broker capacity
- Interface: `com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation`
- Can suggest rightsizing cluster

**Broker Set**
- Logical grouping of brokers
- Used for multi-tenancy or isolation requirements
- Goal: BrokerSetAwareGoal

**Model Training Samples**
- Historical metric data used for capacity estimation
- Stored in `__KafkaCruiseControlModelTrainingSamples` topic
- Helps estimate CPU capacity when not explicitly configured

**Regression Model**
- Statistical model for resource usage prediction
- Used when actual capacity unknown
- Based on historical metric correlations

**Action Acceptance**
- Decision on whether a balancing action is allowed
- Returned by goals when evaluating proposals
- Values: ACCEPT, REPLICA_REJECT, BROKER_REJECT

**Balancing Action**
- Proposed change to cluster state
- Types: Replica movement, Leadership transfer, Replica swap
- Class: `com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction`

**Optimization Options**
- Parameters controlling optimization behavior
- Includes: goal list, excluded topics, excluded brokers, etc.
- Class: `com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions`

**Executor State**
- Finite state machine tracking execution progress
- Prevents concurrent executions
- Provides progress visibility via `/state` endpoint

**Concurrency Adjuster**
- Dynamically adjusts execution concurrency
- Based on cluster health (ISR status, metrics)
- Can slow down or stop execution if cluster stressed

**Replica Swap**
- Exchange replicas between two brokers
- More efficient than two separate moves
- Reduces total data transfer

**Rebalance Disk Mode**
- Special mode for intra-broker (JBOD) optimization
- Uses intra-broker goals only
- Endpoint: POST `/rebalance?rebalance_disk=true`

**Kafka Assigner Mode**
- Compatibility mode with legacy kafka-reassign-partitions tool
- Uses KafkaAssigner-specific goals
- Endpoint: POST `/rebalance?kafka_assigner=true`

## REST API Terms

**Endpoint**
- REST API operation (20 total)
- Enum: `com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint`

**Servlet Parameters**
- Query/POST parameters for API requests
- 33 dedicated parameter classes for validation

**Servlet Response**
- Structured API response objects
- 24 response classes for different endpoints

**Session**
- Temporary state for multi-step operations
- Expires after `webserver.session.maxExpiryTimeMs`

**Access Log**
- Record of all API requests
- Stored in `kafkacruisecontrol-request.log`

**Operation Log**
- Record of all operations (rebalance, add broker, etc.)
- Stored in `kafkacruisecontrol-operation.log`

## Security Terms

**Security Provider**
- Pluggable authentication mechanism
- Interface: `com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider`
- Implementations: JWT, SPNEGO, Basic, TrustedProxy

**JWT (JSON Web Token)**
- Token-based authentication
- Provider: JwtSecurityProvider
- Uses Nimbus JOSE JWT library

**SPNEGO**
- Kerberos-based authentication
- Provider: SpnegoSecurityProvider
- For enterprise environments

**Trusted Proxy**
- Authentication via reverse proxy headers
- Provider: TrustedProxySecurityProvider
- Proxy sets user identity headers

**User Permissions**
- Role-based access control (RBAC)
- Managed by UserPermissionsManager
- Controls API endpoint access

## Performance Terms

**Proposal Cache**
- Pre-computed optimization proposals
- Cached for `proposal.expiration.ms` (default: 60 seconds)
- Allows fast response to `/proposals` endpoint

**Broker Load Caching**
- Cached broker statistics
- Invalidated on cluster model changes
- Improves API response time

**Generation**
- Monotonically increasing counter for cluster model versions
- Used to detect stale data
- Cluster model generation vs. cached data generation

---

**Total Unique Terms:** ~150+
**Core Concepts:** ~50
**Advanced Concepts:** ~100

This glossary provides a comprehensive reference for understanding Cruise Control internals. For code references, see the repository structure document.
