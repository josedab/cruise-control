# Monitoring Cruise Control

This guide covers monitoring and observability for Cruise Control in production.

## Health Checks

### 1. Basic Health Check

```bash
curl -X GET "http://localhost:9090/kafkacruisecontrol/state"
```

**Key Indicators:**

```json
{
  "MonitorState": {
    "state": "RUNNING",              // Should be RUNNING
    "numValidPartitions": 1000,
    "numTotalPartitions": 1000,
    "numValidSnapshotWindows": 24,   // Should be > 0
    "monitoringCoveragePercentage": 100.0  // Should be close to 100%
  },
  "ExecutorState": {
    "state": "NO_TASK_IN_PROGRESS",  // Or other valid state
    "numFinishedPartitionMovements": 0,
    "numInProgressPartitionMovements": 0
  },
  "AnomalyDetectorState": {
    "state": "RUNNING"
  }
}
```

### 2. Partition Coverage Check

**Healthy:** `monitoringCoveragePercentage >= 95%`

```bash
# Check partition coverage
curl -s "http://localhost:9090/kafkacruisecontrol/state" | \
  jq '.MonitorState.monitoringCoveragePercentage'
```

**If coverage is low (<95%):**
1. Check `__CruiseControlMetrics` topic has data
2. Verify metrics reporter configured on all brokers
3. Check LoadMonitor logs for errors

### 3. Snapshot Windows Check

**Healthy:** `numValidSnapshotWindows >= 1` (minimum), ideally 24+

```bash
# Check snapshot windows
curl -s "http://localhost:9090/kafkacruisecontrol/state" | \
  jq '.MonitorState.numValidSnapshotWindows'
```

## JMX Metrics

Cruise Control exposes detailed metrics via JMX.

### Enable JMX

**Edit `kafka-cruise-control-start.sh`:**

```bash
export JMX_PORT=9999
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$JMX_PORT"
```

### Key Metrics

#### LoadMonitor Metrics

```
kafka.cruisecontrol:type=LoadMonitor,name=ValidPartitionRatio
- Description: Percentage of partitions with valid metrics
- Alert: < 0.95 (95%)

kafka.cruisecontrol:type=LoadMonitor,name=NumValidSnapshotWindows
- Description: Number of valid metric snapshot windows
- Alert: < 1

kafka.cruisecontrol:type=LoadMonitor,name=TotalMonitoringTime
- Description: Time spent building ClusterModel (ms)
- Alert: > 300000 (5 minutes)
```

#### Executor Metrics

```
kafka.cruisecontrol:type=Executor,name=state
- Description: Current execution state
- Values: NO_TASK_IN_PROGRESS, STARTING_EXECUTION,
          INTER_BROKER_REPLICA_MOVEMENT, LEADER_MOVEMENT

kafka.cruisecontrol:type=Executor,name=numInProgressPartitionMovements
- Description: Currently executing partition movements
- Alert: Stuck at same value for > 30 minutes

kafka.cruisecontrol:type=Executor,name=numAbortedPartitionMovements
- Description: Failed partition movements
- Alert: > 0 (investigate failures)
```

#### AnomalyDetector Metrics

```
kafka.cruisecontrol:type=AnomalyDetector,name=numSelfHealingStarted
- Description: Number of self-healing actions triggered
- Monitor: Track self-healing frequency

kafka.cruisecontrol:type=AnomalyDetector,name=numAnomaliesDetected
- Description: Total anomalies detected
- Monitor: Unexpected spikes may indicate cluster issues
```

### Collecting JMX Metrics

#### Using JConsole

```bash
jconsole localhost:9999
# Navigate to MBeans → kafka.cruisecontrol
```

#### Using Prometheus JMX Exporter

**Download JMX Exporter:**

```bash
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.0/jmx_prometheus_javaagent-0.17.0.jar
```

**Create `jmx_exporter_config.yml`:**

```yaml
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  - pattern: kafka.cruisecontrol<type=(.+), name=(.+)><>Value
    name: kafka_cruisecontrol_$1_$2
    type: GAUGE
```

**Edit `kafka-cruise-control-start.sh`:**

```bash
export KAFKA_JMX_OPTS="-javaagent:jmx_prometheus_javaagent-0.17.0.jar=8080:jmx_exporter_config.yml"
```

**Scrape metrics:**

```bash
curl http://localhost:8080/metrics
```

**Add to Prometheus `prometheus.yml`:**

```yaml
scrape_configs:
  - job_name: 'cruise-control'
    static_configs:
      - targets: ['localhost:8080']
```

## Log Monitoring

### Log Locations

```
logs/
├── kafkaCruiseControl.out     # Main application log
├── kafkaCruiseControl.err     # Error log
└── kafka-cruise-control-*.log # Rotated logs
```

### Important Log Patterns

#### Errors to Alert On

```bash
# Monitor for errors
tail -f logs/kafkaCruiseControl.out | grep "ERROR"
```

**Critical Errors:**

```
[ERROR] Failed to build cluster model
→ Check Kafka connectivity, metrics topic

[ERROR] Execution failed
→ Check executor state, investigate failure

[ERROR] Anomaly detection failed
→ Check anomaly detector configuration
```

#### Warnings to Monitor

```
[WARN] Partition [topic-0] stuck in ISR update
→ Investigate stuck partition movement

[WARN] ValidPartitionRatio below threshold
→ Check metrics collection

[WARN] Not enough valid snapshot windows
→ Wait for more metrics, or investigate collection issues
```

### Log Aggregation

#### Using ELK Stack

**Filebeat configuration:**

```yaml
filebeat.inputs:
  - type: log
    paths:
      - /path/to/cruise-control/logs/kafkaCruiseControl.out
    fields:
      service: cruise-control
      environment: production

output.elasticsearch:
  hosts: ["localhost:9200"]
```

#### Using Splunk

```bash
# Add to Splunk inputs.conf
[monitor:///path/to/cruise-control/logs/kafkaCruiseControl.out]
sourcetype = cruise_control
index = kafka
```

## Alerts

### Recommended Alerts

#### Critical Alerts (Page Immediately)

1. **Cruise Control Down**
   ```
   Alert: HTTP endpoint not responding
   Condition: GET http://localhost:9090/kafkacruisecontrol/state returns error
   Action: Restart Cruise Control, investigate logs
   ```

2. **Low Partition Coverage**
   ```
   Alert: ValidPartitionRatio < 0.9
   Condition: monitoringCoveragePercentage < 90%
   Action: Check metrics reporter on all brokers
   ```

3. **Execution Stuck**
   ```
   Alert: Partition movement stuck
   Condition: numInProgressPartitionMovements unchanged for > 30 minutes
   Action: Investigate stuck partitions, consider stopping execution
   ```

#### Warning Alerts (Investigate Soon)

1. **Slow ClusterModel Build**
   ```
   Alert: ClusterModel build taking too long
   Condition: TotalMonitoringTime > 300000 (5 minutes)
   Action: Tune LoadMonitor configuration, consider pruning metrics
   ```

2. **High Anomaly Rate**
   ```
   Alert: Too many anomalies detected
   Condition: numAnomaliesDetected > threshold in 1 hour
   Action: Investigate cluster health, review anomaly detector configuration
   ```

3. **Failed Partition Movements**
   ```
   Alert: Partition movements failing
   Condition: numAbortedPartitionMovements > 0
   Action: Review execution logs, check Kafka broker health
   ```

### Sample Prometheus Alerts

**Create `cruise-control-alerts.yml`:**

```yaml
groups:
  - name: cruise-control
    interval: 30s
    rules:
      - alert: CruiseControlDown
        expr: up{job="cruise-control"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Cruise Control is down"
          description: "Cruise Control has been down for more than 5 minutes"

      - alert: LowPartitionCoverage
        expr: kafka_cruisecontrol_loadmonitor_validpartitionratio < 0.9
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Low partition coverage"
          description: "Only {{ $value }}% of partitions have valid metrics"

      - alert: ExecutionStuck
        expr: rate(kafka_cruisecontrol_executor_numfinishedpartitionmovements[30m]) == 0
           and kafka_cruisecontrol_executor_numinprogresspartitionmovements > 0
        for: 30m
        labels:
          severity: critical
        annotations:
          summary: "Partition movements stuck"
          description: "No partition movements completed in 30 minutes"
```

## Dashboards

### Grafana Dashboard

**Import dashboard JSON:**

```json
{
  "dashboard": {
    "title": "Cruise Control Monitoring",
    "panels": [
      {
        "title": "Partition Coverage",
        "targets": [
          {
            "expr": "kafka_cruisecontrol_loadmonitor_validpartitionratio * 100"
          }
        ]
      },
      {
        "title": "ClusterModel Build Time",
        "targets": [
          {
            "expr": "kafka_cruisecontrol_loadmonitor_totalmonitoringtime / 1000"
          }
        ]
      },
      {
        "title": "Partition Movements",
        "targets": [
          {
            "expr": "kafka_cruisecontrol_executor_numinprogresspartitionmovements",
            "legendFormat": "In Progress"
          },
          {
            "expr": "kafka_cruisecontrol_executor_numfinishedpartitionmovements",
            "legendFormat": "Finished"
          }
        ]
      }
    ]
  }
}
```

### Key Dashboard Metrics

1. **Partition Coverage Over Time** - Should stay near 100%
2. **ClusterModel Build Time** - Track performance
3. **Partition Movements** - Visualize execution progress
4. **Anomaly Detection Rate** - Monitor cluster health
5. **JVM Memory Usage** - Prevent OutOfMemory errors

## Performance Monitoring

### JVM Monitoring

**Key JVM Metrics:**

```bash
# Heap usage
jstat -gc <cruise-control-pid> 1000

# Thread count
jstack <cruise-control-pid> | grep "thread count"
```

**Heap Alerts:**
- Heap usage consistently > 80%: Increase heap size
- Frequent GC pauses > 1s: Tune GC settings

### Database/Topic Monitoring

**Monitor metrics storage topics:**

```bash
# Check topic size
kafka-topics --describe --topic __CruiseControlMetrics

# Check consumer lag
kafka-consumer-groups --describe --group CruiseControlConsumer
```

## Troubleshooting Checklist

When Cruise Control has issues:

1. **Check Cruise Control health:** `curl http://localhost:9090/kafkacruisecontrol/state`
2. **Check JMX metrics:** Look for anomalies
3. **Review logs:** Search for ERROR and WARN messages
4. **Check Kafka connectivity:** Verify Cruise Control can reach Kafka
5. **Verify metrics collection:** Check `__CruiseControlMetrics` topic
6. **Check resource usage:** CPU, memory, disk

## Related Documentation

- [Architecture: LoadMonitor](../architecture/01-load-monitor.md)
- [Architecture: Executor](../architecture/03-executor.md)
- [Troubleshooting Guide](troubleshooting.md)
