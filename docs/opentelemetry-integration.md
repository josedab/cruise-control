# OpenTelemetry Distributed Tracing Integration

## Overview

Cruise Control now supports distributed tracing using OpenTelemetry, providing end-to-end observability of rebalance operations. This integration enables tracking of requests through the entire system, from initial API calls through goal optimization and execution.

## Features

With OpenTelemetry tracing enabled, you can:

- **Trace Complete Rebalance Operations**: Track a rebalance request from submission through completion
- **Monitor Goal Optimization**: See individual goal execution times and identify slow goals
- **Track Execution Phases**: Observe inter-broker movements, leadership transfers, and other execution activities
- **Identify Performance Bottlenecks**: Quickly identify which part of the rebalance process is taking the longest
- **Debug Failures**: Capture exceptions and error contexts within traces

## Architecture

The implementation instruments three key components:

1. **KafkaCruiseControl**: Top-level spans for proposal generation and execution
2. **GoalOptimizer**: Spans for overall optimization and individual goal optimization
3. **Executor**: Spans for proposal execution phases

### Trace Hierarchy

A typical rebalance operation produces the following trace structure:

```
POST /rebalance [trace_id: abc123]
├── cruise-control.get-proposals [30m]
│   └── goal-optimizer.optimizations [20m]
│       ├── goal.optimize[RackAwareGoal] [2m]
│       ├── goal.optimize[DiskCapacityGoal] [3m]
│       ├── goal.optimize[CpuCapacityGoal] [2m]
│       └── ... (other goals)
└── cruise-control.execute-proposals [10m]
    └── executor.execute-proposals [10m]
```

## Configuration

### Enabling OpenTelemetry

Add the following configuration to your `cruisecontrol.properties` file:

```properties
# Enable OpenTelemetry distributed tracing
opentelemetry.enabled=true

# OTLP endpoint (Jaeger, Zipkin, or OTLP collector)
opentelemetry.exporter.otlp.endpoint=http://localhost:4317

# Service name for identifying Cruise Control in traces
opentelemetry.service.name=cruise-control
```

### Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `opentelemetry.enabled` | Enable/disable OpenTelemetry tracing | `false` |
| `opentelemetry.exporter.otlp.endpoint` | OTLP gRPC endpoint for trace export | `http://localhost:4317` |
| `opentelemetry.service.name` | Service name in traces | `cruise-control` |

## Setting Up a Tracing Backend

### Option 1: Jaeger (Recommended for Development)

Using Docker:

```bash
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

Access the Jaeger UI at: http://localhost:16686

### Option 2: Zipkin

Using Docker:

```bash
docker run -d --name zipkin \
  -p 9411:9411 \
  openzipkin/zipkin
```

Configure Cruise Control to use Zipkin:

```properties
opentelemetry.enabled=true
opentelemetry.exporter.otlp.endpoint=http://localhost:9411/api/v2/spans
opentelemetry.service.name=cruise-control
```

### Option 3: OTLP Collector

For production environments, use the OpenTelemetry Collector to aggregate and forward traces to your observability platform:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  jaeger:
    endpoint: jaeger:14250
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [jaeger]
```

## Trace Attributes

The implementation adds meaningful attributes to spans for better observability:

### KafkaCruiseControl Spans

- `allow.capacity.estimation`: Whether capacity estimation is allowed
- `proposals.count`: Number of proposals generated
- `execution.uuid`: Unique identifier for the execution
- `kafka.assigner.mode`: Whether Kafka assigner mode is enabled
- `triggered.by.user`: Whether execution was triggered by a user

### GoalOptimizer Spans

- `goals.count`: Number of goals to optimize
- `cluster.brokers`: Number of brokers in the cluster
- `proposals.generated`: Number of proposals generated
- `goals.violated.before`: Count of violated goals before optimization
- `goals.violated.after`: Count of violated goals after optimization

### Individual Goal Spans

- `goal.name`: Name of the goal being optimized
- `goal.duration.ms`: Time taken to optimize the goal
- `goal.succeeded`: Whether goal optimization succeeded
- `goal.has.diff`: Whether the goal produced changes

### Executor Spans

- `proposals.count`: Number of proposals being executed
- `execution.uuid`: Unique identifier for the execution
- `kafka.assigner.mode`: Execution mode
- `triggered.by.user`: Execution trigger source

## Analyzing Traces

### Finding Slow Goals

In your tracing backend (e.g., Jaeger), filter for:
- Service: `cruise-control`
- Operation: `goal.optimize`
- Sort by duration

This will show which goals are taking the longest to optimize.

### Debugging Failed Operations

When a rebalance fails:
1. Search for traces with errors (span.status = ERROR)
2. Examine the exception details recorded in the span
3. Review the span hierarchy to understand at which stage the failure occurred

### Performance Optimization

Use trace data to:
- Identify goals that consistently take long to optimize
- Determine if proposal generation or execution is the bottleneck
- Monitor trends over time to detect performance regressions

## Performance Impact

OpenTelemetry instrumentation has minimal performance overhead:
- Span creation: ~1-5 microseconds
- Attribute addition: ~0.5 microseconds per attribute
- Trace export is asynchronous (batched)

For most deployments, the observability benefits far outweigh the minimal overhead.

## Troubleshooting

### Traces Not Appearing

1. **Verify OpenTelemetry is enabled**:
   ```properties
   opentelemetry.enabled=true
   ```

2. **Check endpoint connectivity**:
   ```bash
   telnet localhost 4317
   ```

3. **Review logs for OpenTelemetry errors**:
   ```bash
   grep -i "opentelemetry" cruise-control.log
   ```

### Incomplete Traces

If traces appear incomplete:
- Verify all components (KafkaCruiseControl, GoalOptimizer, Executor) are instrumented
- Check that spans are properly closed (no exceptions during span lifecycle)
- Ensure adequate buffer size in your OTLP collector

## Best Practices

1. **Use Consistent Service Names**: Keep `opentelemetry.service.name` consistent across restarts
2. **Monitor Export Metrics**: Track span export success/failure rates
3. **Set Appropriate Sampling**: For high-traffic deployments, consider implementing sampling
4. **Correlate with Logs**: Include trace IDs in logs for correlation
5. **Regular Trace Review**: Periodically review traces to identify optimization opportunities

## Future Enhancements

Potential future improvements:
- Additional instrumentation for API endpoints
- Load monitor tracing
- Anomaly detector tracing
- Custom span events for key milestones
- Trace context propagation across HTTP boundaries

## References

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [OpenTelemetry Java SDK](https://github.com/open-telemetry/opentelemetry-java)
- [OTLP Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
