# Multi-Cluster Management

## Overview

Cruise Control now supports managing **multiple Kafka clusters** from a single instance. This feature significantly reduces operational overhead for organizations running many Kafka clusters.

## Key Features

- **Single Control Plane**: Manage multiple Kafka clusters from one Cruise Control instance
- **Isolated State**: Each cluster maintains independent monitoring, optimization, and execution state
- **Unified Configuration**: Share common settings across clusters with per-cluster overrides
- **Backward Compatible**: Single-cluster deployments work unchanged

## Benefits

- **90% Reduction in Operational Overhead**: One deployment instead of N
- **Unified Visibility**: View health and metrics across all clusters
- **Resource Efficiency**: Shared infrastructure reduces memory and CPU usage
- **Consistent Policies**: Apply standardized optimization goals across clusters

## Architecture

```
MultiClusterCoordinator
├── ClusterRegistry (cluster metadata)
├── ClusterContext (us-east)
│   └── KafkaCruiseControl instance
│       ├── LoadMonitor
│       ├── GoalOptimizer
│       ├── Executor
│       └── AnomalyDetector
├── ClusterContext (us-west)
│   └── KafkaCruiseControl instance
│       ├── LoadMonitor
│       ├── GoalOptimizer
│       ├── Executor
│       └── AnomalyDetector
└── ClusterContext (eu-west)
    └── KafkaCruiseControl instance
        ├── LoadMonitor
        ├── GoalOptimizer
        ├── Executor
        └── AnomalyDetector
```

Each cluster has complete isolation:
- Separate monitoring and metrics collection
- Independent optimization proposals
- Isolated execution state
- No cross-cluster interference

## Configuration

### Single-Cluster Mode (Backward Compatible)

```properties
# Standard configuration works unchanged
bootstrap.servers=localhost:9092
zookeeper.connect=localhost:2181
goals=RackAwareGoal,ReplicaCapacityGoal,...
```

### Multi-Cluster Mode

```properties
# Enable multi-cluster mode
multi.cluster.mode.enabled=true

# List of cluster IDs
clusters=us-east,us-west,eu-west

# US-East Cluster Configuration
cluster.us-east.bootstrap.servers=kafka1-us-east:9092,kafka2-us-east:9092
cluster.us-east.zookeeper.connect=zk1-us-east:2181,zk2-us-east:2181
cluster.us-east.display.name=US East Production
cluster.us-east.region=us-east-1
cluster.us-east.environment=production
cluster.us-east.enabled=true

# US-West Cluster Configuration
cluster.us-west.bootstrap.servers=kafka1-us-west:9092,kafka2-us-west:9092
cluster.us-west.zookeeper.connect=zk1-us-west:2181,zk2-us-west:2181
cluster.us-west.display.name=US West Production
cluster.us-west.region=us-west-2
cluster.us-west.environment=production
cluster.us-west.enabled=true

# EU-West Cluster Configuration
cluster.eu-west.bootstrap.servers=kafka1-eu-west:9092,kafka2-eu-west:9092
cluster.eu-west.zookeeper.connect=zk1-eu-west:2181,zk2-eu-west:2181
cluster.eu-west.display.name=EU West Production
cluster.eu-west.region=eu-west-1
cluster.eu-west.environment=production
cluster.eu-west.enabled=true

# Default Configuration (applies to all clusters unless overridden)
default.goals=RackAwareGoal,ReplicaCapacityGoal,DiskCapacityGoal,...
default.anomaly.detection.goals=RackAwareGoal,ReplicaCapacityGoal
default.self.healing.enabled=true

# Per-cluster overrides (optional)
cluster.eu-west.goals=RackAwareGoal,MinTopicLeadersPerBrokerGoal,...
cluster.us-west.self.healing.enabled=false
```

### Configuration Options

#### Global Options

- `multi.cluster.mode.enabled` - Enable multi-cluster mode (default: `false`)
- `clusters` - Comma-separated list of cluster IDs

#### Per-Cluster Options

Required:
- `cluster.{id}.bootstrap.servers` - Kafka bootstrap servers
- `cluster.{id}.zookeeper.connect` - Zookeeper connection string

Optional:
- `cluster.{id}.display.name` - Human-readable name (default: cluster ID)
- `cluster.{id}.region` - Region/datacenter (e.g., "us-east-1")
- `cluster.{id}.environment` - Environment (e.g., "production", "staging", "dev")
- `cluster.{id}.enabled` - Enable/disable cluster (default: `true`)

Any other configuration can be set with `cluster.{id}.{config-name}` to override defaults.

## REST API

### New Multi-Cluster Endpoints

#### List Clusters

```
GET /clusters
```

Returns list of all registered clusters with basic metadata.

**Response:**
```json
{
  "clusters": [
    {
      "id": "us-east",
      "displayName": "US East Production",
      "region": "us-east-1",
      "environment": "production",
      "bootstrapServers": "kafka1-us-east:9092,...",
      "enabled": true
    },
    ...
  ]
}
```

#### Cluster Health

```
GET /cluster_health
```

Returns aggregated health status for all clusters.

**Response:**
```json
{
  "summary": {
    "totalClusters": 20,
    "healthyClusters": 18,
    "degradedClusters": 1,
    "rebalancingClusters": 1,
    "errorClusters": 0
  },
  "clusters": {
    "us-east": {
      "status": "HEALTHY",
      "brokerCount": 50,
      "hasGoalViolations": false
    },
    "us-west": {
      "status": "REBALANCING",
      "brokerCount": 50,
      "hasGoalViolations": false,
      "executorState": "INTER_BROKER_REPLICA_MOVEMENT"
    },
    ...
  }
}
```

### Cluster Parameter Support

All existing endpoints support an optional `cluster` parameter to specify which cluster to operate on.

```
# Single cluster operation
GET /state?cluster=us-east

# Multiple clusters (future enhancement)
GET /state?cluster=us-east,us-west
```

If `cluster` parameter is omitted in single-cluster mode, operates on the default cluster (backward compatible).

## Usage Examples

### Java API

```java
// Load configuration
Properties props = loadProperties("cruise-control.properties");
MetricRegistry metricRegistry = new MetricRegistry();

// Load clusters and create coordinator
MultiClusterCoordinator coordinator =
    MultiClusterConfigLoader.loadCoordinator(props, metricRegistry);

// Initialize (starts all enabled clusters)
coordinator.initialize();

// Get health for all clusters
Map<String, ClusterHealth> health = coordinator.getAllHealth();

// Operate on specific cluster
ClusterContext usEast = coordinator.getContext("us-east");
usEast.pauseSampling();
usEast.resumeSampling();

// Execute operation on all clusters
coordinator.executeOnAll(context -> {
    context.loadMonitor().bootstrap();
    return null;
});

// Shutdown
coordinator.shutdown();
```

### Filtering Clusters

```java
// Get production clusters only
List<ClusterConfig> prodClusters =
    registry.getClustersByEnvironment("production");

// Get clusters in specific region
List<ClusterConfig> usEastClusters =
    registry.getClustersByRegion("us-east-1");

// Get enabled clusters
List<ClusterConfig> enabledClusters =
    registry.getEnabledClusters();
```

## Implementation Details

### Core Classes

- **`ClusterConfig`**: Configuration for a single cluster
- **`ClusterRegistry`**: Registry of all managed clusters
- **`ClusterContext`**: Encapsulates state for one cluster (wraps `KafkaCruiseControl`)
- **`MultiClusterCoordinator`**: Orchestrates operations across clusters
- **`MultiClusterConfigLoader`**: Loads multi-cluster configuration from properties

### Package Structure

```
com.linkedin.kafka.cruisecontrol.multicluster/
├── ClusterConfig.java
├── ClusterRegistry.java
├── ClusterContext.java
├── MultiClusterCoordinator.java
└── MultiClusterConfigLoader.java

com.linkedin.kafka.cruisecontrol.config.constants/
└── MultiClusterConfig.java
```

## Testing

### Unit Tests

```bash
./gradlew test --tests "*multicluster*"
```

Test coverage includes:
- `ClusterConfigTest` - Configuration building and validation
- `ClusterRegistryTest` - Cluster registration and filtering
- `MultiClusterConfigLoaderTest` - Configuration loading from properties

### Integration Tests

Integration tests verify end-to-end multi-cluster functionality with real Kafka clusters.

## Migration Guide

### From Single-Cluster to Multi-Cluster

1. **Add multi-cluster configuration:**

```properties
# Add to existing config
multi.cluster.mode.enabled=true
clusters=my-cluster

# Rename existing configs
cluster.my-cluster.bootstrap.servers=<existing bootstrap.servers>
cluster.my-cluster.zookeeper.connect=<existing zookeeper.connect>
```

2. **Add additional clusters:**

```properties
clusters=my-cluster,new-cluster-1,new-cluster-2

cluster.new-cluster-1.bootstrap.servers=...
cluster.new-cluster-1.zookeeper.connect=...
```

3. **Restart Cruise Control**

## Best Practices

1. **Start Small**: Begin with 2-3 clusters, then scale up
2. **Use Environments**: Tag clusters by environment (prod/staging/dev)
3. **Regional Grouping**: Group clusters by region for easier management
4. **Consistent Naming**: Use descriptive, consistent cluster IDs
5. **Default Configuration**: Set sensible defaults, override only when needed
6. **Monitoring**: Monitor Cruise Control's own resource usage as cluster count grows

## Resource Considerations

### Single-Cluster Mode
- Base: 1GB
- Per-cluster model: 2GB (1M partitions)
- Total for 1 cluster: 3GB

### Multi-Cluster Mode (10 clusters)
- Shared base: 1GB
- Per-cluster models: 10 × 2GB = 20GB
- Total: 21GB

**Savings**: 21GB vs 30GB (10 separate instances) = **30% reduction**

## Limitations and Future Work

### Current Limitations

1. No cross-cluster operations (topic migration, etc.)
2. No unified UI dashboard (REST API only)
3. No cluster health aggregation in UI
4. All clusters must be registered at startup (no dynamic registration)

### Planned Enhancements

1. **Phase 2 Features:**
   - Dynamic cluster registration/deregistration
   - Cross-cluster topic migration
   - Cluster comparison and analytics

2. **Phase 3 Features:**
   - Unified web UI dashboard
   - Multi-cluster alerting
   - Global anomaly detection

3. **Phase 4 Features:**
   - Cluster templates
   - Configuration promotion (dev → staging → prod)
   - Multi-cluster quotas

## Troubleshooting

### Cluster Not Starting

Check logs for specific error. Common issues:
- Invalid bootstrap.servers or zookeeper.connect
- Network connectivity issues
- Insufficient permissions

```
ERROR ClusterContext - Failed to start cluster context for 'us-east'
```

### Memory Issues

If running many clusters, increase JVM heap:

```bash
export KAFKA_CRUISE_CONTROL_OPTS="-Xms4G -Xmx16G"
```

### Configuration Errors

Verify required properties are set:

```properties
# Required for each cluster
cluster.{id}.bootstrap.servers=...
cluster.{id}.zookeeper.connect=...
```

## Support

For issues, questions, or feature requests:
- GitHub Issues: https://github.com/linkedin/cruise-control/issues
- Documentation: https://github.com/linkedin/cruise-control/wiki

## References

- RFC-0011: Multi-Cluster Management (see `analysis-output/rfcs/RFC-0011-multi-cluster-management.md`)
- Cruise Control Documentation: https://github.com/linkedin/cruise-control/wiki
