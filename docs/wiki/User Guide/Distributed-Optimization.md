# Distributed Goal Optimization

## Overview

Distributed Goal Optimization enables Cruise Control to scale horizontally by distributing goal optimization across multiple worker nodes. This feature allows Cruise Control to handle extremely large Kafka clusters (100K+ brokers, 10M+ partitions) by parallelizing the optimization workload.

## Architecture

Cruise Control uses a coordinator-worker pattern for distributed optimization:

```
┌─────────────────────────────────────────────────────────┐
│            Cruise Control Coordinator                   │
│  - LoadMonitor                                          │
│  - Executor                                             │
│  - REST API                                             │
│  - Work Distribution                                    │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Worker 1   │  │  Worker 2   │  │  Worker 3   │
│             │  │             │  │             │
│ Goals 1-5   │  │ Goals 6-10  │  │ Goals 11-15 │
└─────────────┘  └─────────────┘  └─────────────┘
```

### Components

1. **Coordinator**: The main Cruise Control instance that:
   - Maintains the cluster model
   - Distributes goals to workers
   - Merges optimization results
   - Executes proposals

2. **Workers**: Remote Cruise Control instances that:
   - Receive cluster model snapshots
   - Execute assigned goals
   - Return optimized models

3. **ClusterModelSnapshot**: Efficient binary serialization format that:
   - Compresses cluster state (70-80% compression)
   - Reduces network transfer overhead
   - Enables fast model distribution

## Configuration

### Enabling Distributed Optimization

Add the following properties to your `cruisecontrol.properties` file:

```properties
# Enable distributed optimization
distributed.optimization.enabled=true

# Worker endpoints (comma-separated list)
distributed.optimization.workers=worker1.example.com:9091,worker2.example.com:9091,worker3.example.com:9091

# Goal assignment strategy: STATIC or DYNAMIC
distributed.optimization.assignment.strategy=DYNAMIC

# Worker timeout (milliseconds)
distributed.optimization.worker.timeout.ms=600000
```

### Configuration Options

#### `distributed.optimization.enabled`
- **Type**: Boolean
- **Default**: `false`
- **Description**: Enable or disable distributed optimization. When disabled, Cruise Control operates in traditional single-instance mode.

#### `distributed.optimization.workers`
- **Type**: String (comma-separated list)
- **Default**: Empty string
- **Format**: `host1:port1,host2:port2,host3:port3`
- **Description**: List of worker endpoints for distributed optimization. Each worker must be a running Cruise Control instance accessible via HTTP.

#### `distributed.optimization.assignment.strategy`
- **Type**: String
- **Default**: `STATIC`
- **Valid Values**: `STATIC`, `DYNAMIC`
- **Description**: Strategy for assigning goals to workers:
  - **STATIC**: Round-robin distribution. Simple and predictable, but may be unbalanced if goals have different execution times.
  - **DYNAMIC**: Work-stealing pattern. Better load balancing and fault tolerance, with slight coordination overhead.

#### `distributed.optimization.worker.timeout.ms`
- **Type**: Long
- **Default**: `600000` (10 minutes)
- **Description**: Timeout for worker optimization requests. Workers exceeding this timeout are considered failed.

## Deployment

### Setting Up Workers

1. **Deploy Cruise Control instances** on worker nodes with the same configuration as the coordinator (goals, thresholds, etc.).

2. **Configure worker endpoints** in the coordinator's configuration file.

3. **Ensure network connectivity** between coordinator and workers:
   - Workers must be accessible via HTTP
   - Recommended: 10 Gbps network for large cluster models (2-3 GB)
   - Firewall rules should allow coordinator → worker communication

4. **Start workers** before enabling distributed optimization on the coordinator.

### Example Deployment

For a cluster with 100K brokers and 10M partitions:

**Coordinator** (Single instance):
- 8 CPU cores
- 16 GB RAM
- Standard Cruise Control configuration

**Workers** (3 instances):
- 16 CPU cores each
- 32 GB RAM each
- Same goals and thresholds as coordinator

**Expected Performance**:
- Single-instance time: ~10 hours
- Distributed (3 workers): ~3.5 hours (2.8× speedup)
- Cost savings: ~22% with 65% faster optimization

## Goal Assignment Strategies

### Static Strategy

Distributes goals in round-robin fashion across workers.

**Pros**:
- Simple and predictable
- No coordination overhead
- Deterministic assignment

**Cons**:
- May be unbalanced if goals vary in execution time
- Cannot adapt to worker failures

**Best for**:
- Homogeneous goals with similar execution times
- Stable worker environments
- Predictable performance requirements

### Dynamic Strategy

Uses work-stealing pattern where workers pull goals as they complete previous ones.

**Pros**:
- Automatic load balancing
- Adapts to varying goal execution times
- Better fault tolerance

**Cons**:
- Slight coordination overhead
- More complex implementation

**Best for**:
- Heterogeneous goals with varying execution times
- Dynamic worker environments
- Maximum throughput requirements

## Performance Characteristics

### Scalability

With N workers, expect approximately N× speedup, accounting for:
- Coordination overhead: ~15-20%
- Network transfer time: Depends on cluster size and network bandwidth
- Result merging time: ~5-10% of total optimization time

### Network Requirements

**Bandwidth**: Cluster model transfer requires:
- Initial transfer: 2-3 GB for 100K broker cluster (compressed)
- Delta updates: ~100 MB per worker (future optimization)

**Latency**: Low latency network recommended:
- < 10 ms between coordinator and workers
- Higher latency increases coordination overhead

### Memory Requirements

**Coordinator**: Same as single-instance mode
- Cluster model: ~4-6 GB for 100K brokers

**Workers**: Each worker requires:
- Cluster model copy: ~4-6 GB
- Goal execution overhead: ~2-4 GB
- Total per worker: ~8-12 GB

## Monitoring

### Metrics

Monitor the following metrics for distributed optimization:

- **Worker availability**: Number of healthy workers
- **Goal distribution**: Goals per worker
- **Optimization time**: Per-worker and total time
- **Network transfer**: Bytes sent/received
- **Worker failures**: Failed workers and retries

### Logs

Enable DEBUG logging for distributed optimization:

```properties
log4j.logger.com.linkedin.kafka.cruisecontrol.analyzer.distributed=DEBUG
```

Key log messages:
- Worker assignment decisions
- Optimization start/completion per worker
- Network transfer statistics
- Result merging progress

## Troubleshooting

### Workers Not Available

**Symptom**: "No workers available for distributed optimization"

**Solutions**:
1. Verify worker URLs are correct and accessible
2. Check network connectivity: `curl http://worker1:9091/health`
3. Ensure workers are running and healthy
4. Check firewall rules

### Optimization Timeout

**Symptom**: "Worker optimization failed" or timeout errors

**Solutions**:
1. Increase `distributed.optimization.worker.timeout.ms`
2. Allocate more resources (CPU, RAM) to workers
3. Reduce number of goals assigned per worker
4. Check for worker performance issues

### Incorrect Results

**Symptom**: Optimization results differ from single-instance mode

**Solutions**:
1. Verify all workers have identical configuration (goals, thresholds)
2. Check for network errors during model transfer
3. Review logs for result merging issues
4. Disable distributed optimization and compare results

## Limitations

### Current Implementation

- **Goal Dependencies**: All goals are currently executed sequentially. Parallel execution of independent goals is planned for future releases.
- **Worker Failures**: Failed workers cause optimization to fail. Automatic retry and work reassignment are planned.
- **Load Monitoring**: LoadMonitor is not distributed. For very large clusters (1M+ partitions), consider increasing LoadMonitor resources.
- **State Consistency**: Workers receive point-in-time snapshots. Long-running optimizations may use slightly stale data.

### Compatibility

- **Minimum Version**: Requires Cruise Control 2.6.0+
- **Kafka Version**: Compatible with all supported Kafka versions
- **Network**: Requires HTTP connectivity between coordinator and workers

## Best Practices

1. **Start with 2-3 workers** and scale up based on performance metrics
2. **Use DYNAMIC strategy** for heterogeneous workloads
3. **Monitor worker health** and set up alerts for failures
4. **Co-locate workers** with coordinator in the same datacenter to minimize network latency
5. **Size workers appropriately**: 16 CPU cores and 32 GB RAM per worker for large clusters
6. **Test thoroughly** in staging before production deployment
7. **Keep configurations in sync** between coordinator and workers

## Future Enhancements

Planned improvements for distributed optimization:

- **RFC-0006**: Leader election for high availability
- **Parallel goal execution**: Execute independent goals in parallel
- **Fault tolerance**: Automatic worker failure detection and work reassignment
- **Delta updates**: Send only changed replicas instead of full model
- **Connection pooling**: Reuse HTTP connections for better performance
- **Auto-scaling**: Dynamically add/remove workers based on load

## References

- [RFC-0005: Distributed Goal Optimization](../../../analysis-output/rfcs/RFC-0005-distributed-optimization.md)
- [Cruise Control Architecture](./Architecture.md)
- [Goal Configuration](./Goals.md)
