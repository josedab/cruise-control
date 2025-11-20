# ClusterModel: In-Memory Cluster State Representation

## Overview

The ClusterModel is an in-memory representation of the entire Kafka cluster state, including brokers, partitions, replicas, and their associated metrics. It serves as the foundation for all optimization operations in Cruise Control.

## Structure

```
ClusterModel
├── Brokers (Map<Integer, Broker>)
│   ├── Broker (id=0)
│   │   ├── Capacity (CPU, Disk, Network In/Out)
│   │   ├── Load (Current utilization)
│   │   ├── Replicas (Set<Replica>)
│   │   └── Rack (String)
│   ├── Broker (id=1)
│   └── ...
├── Replicas (Map<TopicPartition, List<Replica>>)
│   ├── TopicPartition (topic-0, partition-0)
│   │   ├── Replica (broker=0, leader=true)
│   │   ├── Replica (broker=1, leader=false)
│   │   └── Replica (broker=2, leader=false)
│   └── ...
└── Racks (Map<String, Rack>)
    ├── Rack (rack-1)
    │   └── Brokers [0, 1, 2]
    └── ...
```

## Core Entities

### 1. Broker

Represents a Kafka broker with its capacity and current load.

**Key Fields:**
```java
class Broker {
  int id;
  Host host;
  Rack rack;
  BrokerCapacity capacity;     // Max resources
  Load load;                   // Current utilization
  Set<Replica> replicas;       // Replicas hosted
  boolean isAlive;
  Map<Resource, Double> utilizationPercentage;
}
```

**Capacity:**
- `DISK`: Total disk capacity (GB)
- `CPU`: CPU capacity (percentage or cores)
- `NW_IN`: Network inbound bandwidth (MB/s)
- `NW_OUT`: Network outbound bandwidth (MB/s)

**Load:**
- Aggregation of all replica loads on this broker
- Updated from metrics every 120 seconds
- Includes: bytes in/out, CPU usage, disk usage

**Example:**
```json
{
  "id": 0,
  "host": "kafka-broker-0.example.com:9092",
  "rack": "us-east-1a",
  "capacity": {
    "DISK": 1000.0,      // GB
    "CPU": 100.0,        // Percentage
    "NW_IN": 125.0,      // MB/s (1 Gbps)
    "NW_OUT": 125.0
  },
  "load": {
    "DISK": 450.0,       // GB used
    "CPU": 60.0,         // 60% CPU
    "NW_IN": 75.0,       // MB/s
    "NW_OUT": 80.0
  },
  "numReplicas": 150,
  "numLeaders": 50
}
```

### 2. Replica

Represents a partition replica with its metrics.

**Key Fields:**
```java
class Replica {
  TopicPartition topicPartition;
  Broker broker;
  boolean isLeader;
  Load load;                      // Resource usage
  List<Long> currentOffsets;      // Offset per time window
  boolean isCurrentOffline;
}
```

**Load Metrics:**
- `DISK`: Disk space used by this partition (GB)
- `CPU`: CPU usage for replication/produce/consume
- `NW_IN`: Bytes produced per second (MB/s)
- `NW_OUT`: Bytes consumed/replicated per second (MB/s)

**Leader vs. Follower:**
- **Leader:** Handles all client requests (produce/consume)
  - Higher CPU and network usage
  - `NW_IN` = produce rate
  - `NW_OUT` = consume rate + replication to followers

- **Follower:** Only replicates from leader
  - Lower resource usage
  - `NW_IN` = replication rate
  - `NW_OUT` = 0 (no consumer traffic)

**Example:**
```json
{
  "topic": "high-throughput-topic",
  "partition": 0,
  "broker": 0,
  "isLeader": true,
  "load": {
    "DISK": 15.0,        // GB
    "CPU": 5.0,          // 5% CPU
    "NW_IN": 10.0,       // MB/s produce rate
    "NW_OUT": 12.0       // MB/s (consume + replication)
  }
}
```

### 3. Rack

Logical grouping of brokers for rack awareness.

**Key Fields:**
```java
class Rack {
  String id;
  Set<Broker> brokers;
}
```

**Purpose:**
- Ensure replicas spread across racks for fault tolerance
- `RackAwareGoal` enforces rack-aware placement

**Example:**
```json
{
  "rack-1": ["broker-0", "broker-1", "broker-2"],
  "rack-2": ["broker-3", "broker-4", "broker-5"],
  "rack-3": ["broker-6", "broker-7", "broker-8"]
}
```

## Resource Types

Cruise Control tracks five resource types:

| Resource | Description | Unit | Typical Bottleneck |
|----------|-------------|------|-------------------|
| `DISK` | Disk space usage | GB | High-retention topics |
| `CPU` | CPU utilization | Percentage | Compression, many partitions |
| `NW_IN` | Network inbound (produce) | MB/s | High-throughput producers |
| `NW_OUT` | Network outbound (consume + replication) | MB/s | High-throughput consumers |
| `FOLLOWER_NW_IN` | Follower replication only | MB/s | Large partition movements |

## ClusterModel Operations

### 1. Build from Scratch

```java
// Step 1: Create brokers
for (Node broker : cluster.nodes()) {
  clusterModel.createBroker(
    broker.id(),
    broker.host(),
    broker.rack(),
    brokerCapacity
  );
}

// Step 2: Create replicas
for (TopicPartition tp : cluster.partitions()) {
  for (Integer brokerId : cluster.replicas(tp)) {
    clusterModel.createReplica(
      tp,
      brokerId,
      isLeader
    );
  }
}

// Step 3: Load metrics
for (Replica replica : clusterModel.replicas()) {
  Load load = metricStore.getLoad(replica, timeWindow);
  replica.setLoad(load);
}

// Step 4: Aggregate broker loads
for (Broker broker : clusterModel.brokers()) {
  broker.setLoad(sumReplicaLoads(broker.replicas()));
}
```

**Time Complexity:** O(brokers + partitions × replicas)

### 2. Clone for Optimization

Goals modify ClusterModel in-place, so we need to clone before optimization:

```java
ClusterModel original = loadMonitor.clusterModel();
ClusterModel workingModel = original.clone();

// Goals modify workingModel
for (Goal goal : goals) {
  goal.optimize(workingModel);
}

// Diff to generate proposals
List<ExecutionProposal> proposals = diff(original, workingModel);
```

**Memory Impact:** 2x ClusterModel size during optimization

### 3. Move Replica

Core operation used by goals:

```java
// Move replica from sourceBroker to targetBroker
clusterModel.relocateReplica(
  topicPartition,
  sourceBrokerId,
  targetBrokerId
);

// Updates:
// 1. Remove replica from sourceBroker
// 2. Add replica to targetBroker
// 3. Update broker loads
// 4. Maintain leader status
```

### 4. Transfer Leadership

```java
// Transfer leadership to preferredLeader
clusterModel.transferLeadership(
  topicPartition,
  oldLeader,
  newLeader
);

// Updates:
// 1. Mark old leader as follower
// 2. Mark new leader as leader
// 3. Adjust CPU/network loads (leader has higher load)
```

## Memory Optimization Strategies

### Problem: Large Clusters Use Excessive Memory

**Example:** 1M partitions, RF=3 = 3M replicas × 250KB = 750 GB RAM

### Strategy 1: Time Window Pruning

Only keep recent metrics:

```properties
# Keep 24 hours instead of 7 days
num.partition.metrics.windows=24
partition.metrics.window.ms=3600000
```

**Savings:** ~85% memory reduction (24 hours vs. 168 hours)

### Strategy 2: Lazy Loading

Load metrics on-demand instead of eagerly:

```java
class Replica {
  Load cachedLoad;

  Load getLoad() {
    if (cachedLoad == null) {
      cachedLoad = metricStore.loadMetrics(this);
    }
    return cachedLoad;
  }
}
```

**Trade-off:** Slower first access, but lower memory

### Strategy 3: Sampling

For very large clusters, sample partitions instead of loading all:

```java
// Load metrics for 10% of partitions
Set<TopicPartition> sample = randomSample(allPartitions, 0.1);
for (TopicPartition tp : sample) {
  loadMetrics(tp);
}
```

**Trade-off:** Less accurate optimization, but feasible for huge clusters

## Validation and Consistency

### Invariants

ClusterModel maintains these invariants:

1. **Each partition has exactly RF replicas**
   - If RF=3, partition must have 3 replicas

2. **Each partition has exactly one leader**
   - One replica has `isLeader=true`

3. **Broker load = sum of replica loads**
   - Broker load must equal aggregate of all its replicas

4. **Capacity constraints not violated (soft)**
   - Broker load should not exceed capacity
   - Goals work to fix violations

### Validation

```java
void validate() {
  for (TopicPartition tp : partitions) {
    List<Replica> replicas = getReplicas(tp);

    // Check replication factor
    assert replicas.size() == replicationFactor(tp.topic());

    // Check exactly one leader
    long leaderCount = replicas.stream()
      .filter(Replica::isLeader)
      .count();
    assert leaderCount == 1;
  }

  for (Broker broker : brokers) {
    // Check broker load consistency
    Load aggregated = sumReplicaLoads(broker.replicas());
    assert broker.load().equals(aggregated);
  }
}
```

## Related Documentation

- [Architecture Overview](00-overview.md)
- [LoadMonitor Deep Dive](01-load-monitor.md)
- [GoalOptimizer Internals](02-goal-optimizer.md)
