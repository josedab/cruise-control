# Security and Operational Excellence

**Part 6 of 6** - Production Readiness Guide

**Reading Time:** ~9 minutes | **Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## What You'll Learn

- Security architecture and authentication options
- Observability best practices (metrics, logging, tracing)
- Deployment patterns (Docker, Kubernetes)
- Operational war stories and lessons learned

## Security Architecture

Cruise Control provides multiple authentication mechanisms:

### Option 1: JWT Authentication

**Setup:**

```properties
# In cruisecontrol.properties
webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.JwtSecurityProvider
jwt.authentication.provider.url=https://auth.yourcompany.com
```

**How it works:**

```java
// From JwtSecurityProvider.java
public boolean authenticate(HttpServletRequest request) {
    String authHeader = request.getHeader("Authorization");
    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
        return false;
    }
    
    String token = authHeader.substring(7);
    try {
        // Validate JWT signature and claims
        JWTClaimsSet claims = JWTParser.parse(token).getJWTClaimsSet();
        
        // Check expiration
        if (claims.getExpirationTime().before(new Date())) {
            return false;
        }
        
        // Store user in request
        request.setAttribute("user", claims.getSubject());
        return true;
    } catch (ParseException e) {
        return false;
    }
}
```

**Use case:** Modern microservices, SSO integration

### Option 2: Kerberos (SPNEGO)

**Setup:**

```properties
webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoSecurityProvider
spnego.principal=HTTP/cruise-control.yourcompany.com@REALM
spnego.keytab=/etc/security/cruise-control.keytab
```

**Use case:** Enterprise environments with Active Directory

### Option 3: Trusted Proxy

**Setup:**

```properties
webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.TrustedProxySecurityProvider
trusted.proxy.services=nginx,envoy
trusted.proxy.services.ip.regex=10\.0\..*
```

**How it works:**

```java
// Proxy sets headers
X-Forwarded-User: alice@example.com
X-Forwarded-Groups: kafka-admins,sre

// CruiseControl trusts these headers if from trusted IP
```

**Use case:** Behind API gateway or reverse proxy

### Authorization (Role-Based)

```java
// Configure per-endpoint permissions
public class UserPermissionsManager {
    
    // Admin can do everything
    private static final Set<CruiseControlEndPoint> ADMIN_ENDPOINTS = 
        EnumSet.allOf(CruiseControlEndPoint.class);
    
    // Viewers can only read
    private static final Set<CruiseControlEndPoint> VIEWER_ENDPOINTS = 
        EnumSet.of(
            CruiseControlEndPoint.STATE,
            CruiseControlEndPoint.LOAD,
            CruiseControlEndPoint.PROPOSALS,
            CruiseControlEndPoint.KAFKA_CLUSTER_STATE
        );
    
    public boolean hasPermission(String user, CruiseControlEndPoint endpoint) {
        Set<String> roles = getUserRoles(user);
        
        if (roles.contains("admin")) {
            return ADMIN_ENDPOINTS.contains(endpoint);
        } else if (roles.contains("viewer")) {
            return VIEWER_ENDPOINTS.contains(endpoint);
        }
        return false;
    }
}
```

## Observability: The Three Pillars

### 1. Metrics (JMX)

Cruise Control exports JMX metrics:

```bash
# Enable JMX
export JMX_PORT=56666

# Key metrics to monitor
kafka.cruise.control:type=LoadMonitor,name=MonitoringState    # RUNNING/SAMPLING/BOOTSTRAPPING
kafka.cruise.control:type=Executor,name=ExecutorState         # NO_TASK_IN_PROGRESS/INTER_BROKER_MOVEMENT/...
kafka.cruise.control:type=AnomalyDetector,name=BrokerFailures # Count of detected failures
kafka.cruise.control:type=GoalOptimizer,name=ProposalCacheHitRate  # Cache efficiency
```

**Prometheus integration:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'cruise-control'
    static_configs:
      - targets: ['cruise-control:56666']
    metrics_path: '/metrics'
```

**Key metrics to alert on:**
- `ExecutorState != NO_TASK_IN_PROGRESS` for >1 hour → stuck execution
- `MonitoringState != RUNNING` → metric collection failing
- `ProposalGenerationFailureRate > 0` → goal optimization errors

### 2. Logging (Structured)

**Log configuration** (log4j2.properties):

```properties
# Separate log files for different concerns

# Main application log
appender.kafkaCruiseControlAppender.fileName=./logs/kafkacruisecontrol.log

# Operation audit log (all rebalances, broker adds, etc.)
appender.operationAppender.fileName=./logs/kafkacruisecontrol-operation.log

# API access log (all REST requests)
appender.requestAppender.fileName=./logs/kafkacruisecontrol-request.log
```

**Example operation log entry:**

```json
{
  "timestamp": "2025-11-20T10:30:00Z",
  "operation": "REBALANCE",
  "user": "alice@example.com",
  "goals": ["RackAwareGoal", "DiskCapacityGoal"],
  "duration_ms": 1800000,
  "proposals": 245,
  "status": "COMPLETED"
}
```

**Centralized logging:**

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    paths:
      - /var/log/cruise-control/*.log
    fields:
      service: cruise-control
      environment: production
    multiline.pattern: '^\['
    multiline.negate: true
    multiline.match: after

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
```

### 3. Tracing (Opportunity - RFC-0007)

**Current state:** No distributed tracing

**Proposed:** OpenTelemetry integration

```java
// Instrument key operations
@WithSpan
public OptimizerResult optimizations(ClusterModel model, ...) {
    Span span = Span.current();
    span.setAttribute("cluster.brokers", model.brokers().size());
    span.setAttribute("cluster.partitions", model.partitions().size());
    
    for (Goal goal : goals) {
        Span goalSpan = tracer.spanBuilder("goal.optimize")
            .setAttribute("goal.name", goal.name())
            .startSpan();
        try {
            goal.optimize(model, ...);
        } finally {
            goalSpan.end();
        }
    }
    return result;
}
```

**Benefit:** Trace a rebalance request from API → goals → execution

## Deployment Patterns

### Pattern 1: Docker Deployment

**Dockerfile:**

```dockerfile
FROM eclipse-temurin:17-jre

# Copy Cruise Control
COPY cruise-control /opt/cruise-control
COPY config/cruisecontrol.properties /opt/cruise-control/config/
COPY config/capacity.json /opt/cruise-control/config/

# Expose ports
EXPOSE 9090 56666

# Run
CMD ["/opt/cruise-control/kafka-cruise-control-start.sh", "/opt/cruise-control/config/cruisecontrol.properties"]
```

**docker-compose.yml:**

```yaml
version: '3.8'
services:
  cruise-control:
    image: cruise-control:latest
    ports:
      - "9090:9090"
      - "56666:56666"
    environment:
      KAFKA_HEAP_OPTS: "-Xms4G -Xmx4G"
      JMX_PORT: 56666
    volumes:
      - ./config:/opt/cruise-control/config
      - ./logs:/opt/cruise-control/logs
    depends_on:
      - kafka
```

### Pattern 2: Kubernetes Deployment

**cruise-control-deployment.yaml:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cruise-control
spec:
  replicas: 1  # Only one instance (execution requires single leader)
  selector:
    matchLabels:
      app: cruise-control
  template:
    metadata:
      labels:
        app: cruise-control
    spec:
      containers:
      - name: cruise-control
        image: cruise-control:latest
        ports:
        - containerPort: 9090
          name: http
        - containerPort: 56666
          name: jmx
        env:
        - name: KAFKA_HEAP_OPTS
          value: "-Xms8G -Xmx8G"
        - name: JMX_PORT
          value: "56666"
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "12Gi"
            cpu: "8"
        volumeMounts:
        - name: config
          mountPath: /opt/cruise-control/config
        - name: logs
          mountPath: /opt/cruise-control/logs
        livenessProbe:
          httpGet:
            path: /kafkacruisecontrol/state
            port: 9090
          initialDelaySeconds: 120
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /kafkacruisecontrol/state
            port: 9090
          initialDelaySeconds: 60
          periodSeconds: 10
      volumes:
      - name: config
        configMap:
          name: cruise-control-config
      - name: logs
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: cruise-control
spec:
  selector:
    app: cruise-control
  ports:
  - name: http
    port: 9090
    targetPort: 9090
  - name: jmx
    port: 56666
    targetPort: 56666
  type: LoadBalancer
```

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cruise-control-config
data:
  cruisecontrol.properties: |
    bootstrap.servers=kafka:9092
    metric.sampler.class=com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler
    capacity.config.file=/opt/cruise-control/config/capacity.json
    # ... rest of config
```

### Pattern 3: High Availability (Future - RFC-0006)

**Current limitation:** Single instance only (Executor state isn't distributed)

**Proposed:** Leader election via ZooKeeper/etcd

```java
// Pseudo-code
if (leaderElection.isLeader()) {
    // This instance can execute proposals
    _executor.enable();
} else {
    // Read-only mode: proposals only
    _executor.disable();
}
```

## Operational War Stories

### Story 1: The Infinite Rebalance

**Problem:** Rebalance never completed, ran for 3 days

**Root cause:**
```properties
# Too aggressive concurrency
num.concurrent.partition.movements.per.broker=50  # Too high!
max.num.cluster.partition.movements=5000
```

**Impact:** Cluster network saturated, kafka brokers became unresponsive

**Fix:**
```properties
num.concurrent.partition.movements.per.broker=5
max.num.cluster.partition.movements=500
```

**Lesson:** Conservative defaults exist for a reason. Tune based on your hardware.

### Story 2: The Goal Violation Loop

**Problem:** GoalViolationDetector continuously detected violations

**Root cause:** Capacity file out of date

```json
// capacity.json had:
{"brokerId": 0, "capacity": {"DISK": "1000000"}}  // 1TB

// Actual broker had 2TB disk

// Goals thought broker was 200% over capacity!
```

**Fix:** Update capacity file to match reality

**Lesson:** Keep capacity.json in sync with hardware changes

### Story 3: The Memory Leak

**Problem:** Cruise Control OOM after 2 weeks

**Root cause:** Cached proposals not garbage collected

**Fix:**
```properties
# Expire proposals more aggressively
proposal.expiration.ms=60000  # 1 minute instead of default
```

**Lesson:** Monitor heap usage, tune caching parameters

## Production Checklist

Before deploying Cruise Control to production:

- [ ] **Capacity file accurate** - Matches actual broker hardware
- [ ] **Concurrency tuned** - Conservative initially, increase gradually
- [ ] **Monitoring in place** - JMX metrics scraped, alerts configured
- [ ] **Logging centralized** - Logs shipped to centralized system
- [ ] **Backups configured** - Sample store topics backed up
- [ ] **Security enabled** - JWT/Kerberos authentication configured
- [ ] **RBAC configured** - Not everyone should trigger rebalances!
- [ ] **Two-step verification enabled** - For destructive operations
- [ ] **Self-healing disabled initially** - Enable after you trust it
- [ ] **Tested in staging** - Run actual rebalances, test failure scenarios

## Key Takeaways

1. **Security is multi-layered:** Authentication + authorization + audit logging
2. **Observability is critical:** Metrics, logs, and (soon) tracing
3. **Deploy carefully:** Start with single instance, conservative settings
4. **Monitor constantly:** JMX metrics, log patterns, cluster health
5. **Tune based on experience:** Every cluster is different

## Next Steps

1. **Read all RFCs:** See proposed improvements
2. **Deploy to staging:** Test before production
3. **Join the community:** [GitHub Discussions](https://github.com/linkedin/cruise-control/discussions)
4. **Contribute back:** Share your custom goals, fixes, improvements

---

**Series Complete!** 🎉

You've now learned:
- How Cruise Control works (Posts 1-2)
- Design patterns used (Post 3)
- How to extend it (Post 4)
- Performance considerations (Post 5)
- Production deployment (Post 6)

**Further Reading:**
- [RFC Directory](../rfcs/) - Detailed improvement proposals
- [Diagrams](../diagrams/) - Architecture visualizations
- [Executive Summary](../executive-summary.md) - High-level overview

*Part 6 of 6 | All code references based on commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)*
