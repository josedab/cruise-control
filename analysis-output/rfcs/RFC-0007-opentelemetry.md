# RFC-0007: OpenTelemetry Distributed Tracing

**Status:** Draft | **Priority:** P1 (Strategic)
**Author:** Analysis Team | **Effort:** 15-20 dev-days
**Created:** 2025-11-20

## Summary

Add distributed tracing using OpenTelemetry to enable end-to-end observability of rebalance operations.

## Motivation

**Current Gap:** No way to trace a request through the system

```
User submits /rebalance
  ↓ ??? (black box)
30 minutes later: Done
```

**What we need:**
- Which goals took longest?
- Why did execution slow down?
- Where did it fail?

## Detailed Design

### Add OpenTelemetry Dependency

```gradle
implementation 'io.opentelemetry:opentelemetry-api:1.32.0'
implementation 'io.opentelemetry:opentelemetry-sdk:1.32.0'
implementation 'io.opentelemetry:opentelemetry-exporter-otlp:1.32.0'
```

### Instrument Key Operations

```java
// KafkaCruiseControl.java
@WithSpan("cruise-control.rebalance")
public OptimizerResult rebalance(...) {
    Span span = Span.current();
    span.setAttribute("cluster.brokers", clusterModel.brokers().size());
    span.setAttribute("goals.count", goals.size());
    
    try {
        OptimizerResult result = _goalOptimizer.optimizations(...);
        span.setAttribute("proposals.count", result.proposals().size());
        return result;
    } catch (Exception e) {
        span.recordException(e);
        throw e;
    }
}

// GoalOptimizer.java
for (Goal goal : goals) {
    Span goalSpan = tracer.spanBuilder("goal.optimize")
        .setAttribute("goal.name", goal.name())
        .startSpan();
    try (Scope scope = goalSpan.makeCurrent()) {
        goal.optimize(...);
    } finally {
        goalSpan.end();
    }
}
```

### Trace Visualization

```
POST /rebalance [trace_id: abc123]
├── cruise-control.rebalance [30m]
│   ├── load-monitor.get-model [10s]
│   ├── goal-optimizer.optimizations [20m]
│   │   ├── goal.optimize[RackAwareGoal] [2m]
│   │   ├── goal.optimize[DiskCapacityGoal] [3m]
│   │   ├── goal.optimize[CpuCapacityGoal] [2m]
│   │   └── ... (12 more goals)
│   └── executor.execute [10m]
│       ├── executor.inter-broker-movements [8m]
│       └── executor.leadership-movements [2m]
```

## Configuration

```properties
opentelemetry.enabled=true
opentelemetry.exporter.otlp.endpoint=http://jaeger:4317
```

## Success Criteria

- All API requests traced
- Goal execution times visible
- Trace export to Jaeger/Zipkin

---

**Impact:** Medium | **Effort:** Medium | **Start:** Q1 2026
