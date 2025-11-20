# Quick Start Guide

This guide will get you up and running with Cruise Control in under 10 minutes.

## Prerequisites

- Apache Kafka 2.0 or higher
- Java 11 or higher
- At least 4 GB RAM

## Step 1: Download Cruise Control

```bash
# Clone the repository
git clone https://github.com/linkedin/cruise-control.git
cd cruise-control

# Build
./gradlew jar
```

## Step 2: Configure Kafka Brokers

Cruise Control requires metrics from Kafka brokers. Add the metrics reporter to each broker's configuration.

**Edit `config/server.properties` on each Kafka broker:**

```properties
# Add Cruise Control Metrics Reporter
metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter

# Cruise Control server endpoint
cruise.control.metrics.reporter.bootstrap.servers=localhost:9092

# Topic for metrics (will be auto-created)
cruise.control.metrics.topic=__CruiseControlMetrics

# Metrics reporting interval (60 seconds)
cruise.control.metrics.reporter.metrics.reporting.interval.ms=60000
```

**Copy Cruise Control Metrics Reporter JAR to Kafka libs:**

```bash
# Copy the metrics reporter JAR to Kafka
cp cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter-*.jar \
   /path/to/kafka/libs/
```

**Restart Kafka brokers:**

```bash
bin/kafka-server-stop.sh
bin/kafka-server-start.sh config/server.properties
```

## Step 3: Configure Cruise Control

**Edit `config/cruisecontrol.properties`:**

```properties
# Kafka bootstrap servers
bootstrap.servers=localhost:9092

# Zookeeper connection (required for Kafka < 2.4)
zookeeper.connect=localhost:2181

# Capacity configuration
capacity.config.file=config/capacity.json

# Webserver port
webserver.http.port=9090

# Sample store topics
partition.metric.sample.store.topic=__KafkaCruiseControlPartitionMetricSamples
broker.metric.sample.store.topic=__KafkaCruiseControlModelTrainingSamples
metric.sample.store.topic=__KafkaCruiseControlMetrics
```

**Create `config/capacity.json`:**

```json
{
  "brokerCapacities": [
    {
      "brokerId": "-1",
      "capacity": {
        "DISK": "1000000",
        "CPU": "100",
        "NW_IN": "125000",
        "NW_OUT": "125000"
      },
      "doc": "Default capacity for all brokers (DISK in MB, CPU in %, NW in KB/s)"
    }
  ]
}
```

## Step 4: Start Cruise Control

```bash
./kafka-cruise-control-start.sh config/cruisecontrol.properties
```

**Verify Cruise Control started:**

```bash
# Check logs
tail -f logs/kafkaCruiseControl.out

# Look for:
# [INFO] Starting Cruise Control...
# [INFO] LoadMonitor started
# [INFO] Webserver started on port 9090
```

## Step 5: Access Web UI

Open your browser and navigate to:

```
http://localhost:9090
```

You should see the Cruise Control UI.

## Step 6: Wait for Metrics Collection

Cruise Control needs to collect metrics before it can generate proposals. This takes about 5-10 minutes.

**Check readiness:**

```bash
curl -X GET "http://localhost:9090/kafkacruisecontrol/state"
```

Look for `"MonitorReady": true` in the response.

## Step 7: Generate Rebalance Proposal

Once metrics are ready, generate a rebalance proposal:

```bash
curl -X POST "http://localhost:9090/kafkacruisecontrol/rebalance?dryrun=true&verbose=true"
```

**Response will include:**
- Summary of proposed partition movements
- Expected load distribution after rebalance
- Estimated time to complete

## Step 8: Execute Rebalance (Optional)

If you're satisfied with the proposal, execute it:

```bash
curl -X POST "http://localhost:9090/kafkacruisecontrol/rebalance?dryrun=false"
```

**Response:**
```json
{
  "userTaskId": "12345678-abcd-1234-efgh-123456789012",
  "summary": "Rebalancing cluster with 15 goals"
}
```

**Monitor progress:**

```bash
# Poll for status
curl -X GET "http://localhost:9090/kafkacruisecontrol/user_tasks?user_task_ids=12345678-abcd-1234-efgh-123456789012"
```

## What's Next?

- **Learn about Goals:** See [Configuration Guide](configuration.md) to understand and configure optimization goals
- **Monitor Your Cluster:** See [Operations Guide](operation.md) for monitoring and maintenance
- **Tune Performance:** See [../operations/capacity-planning.md](../operations/capacity-planning.md) for optimization tips
- **Automate Operations:** Set up anomaly detection for self-healing

## Common Issues

### "MonitorReady": false

**Problem:** Cruise Control hasn't collected enough metrics yet.

**Solution:** Wait 5-10 minutes. Check that:
1. Metrics reporter is configured on all brokers
2. `__CruiseControlMetrics` topic exists and has data
3. Cruise Control can connect to Kafka

### Can't connect to Cruise Control UI

**Problem:** Webserver not starting.

**Solution:**
1. Check logs: `tail -f logs/kafkaCruiseControl.out`
2. Verify port 9090 is not in use: `lsof -i :9090`
3. Check `webserver.http.port` in configuration

### "Not enough valid snapshot windows"

**Problem:** Not enough metric samples collected.

**Solution:** Wait longer. Cruise Control needs:
- At least 1 hour of metrics for basic proposals
- 24+ hours for production use

## Related Documentation

- [Configuration Guide](configuration.md)
- [Operations Guide](operation.md)
- [Architecture Overview](../architecture/00-overview.md)
