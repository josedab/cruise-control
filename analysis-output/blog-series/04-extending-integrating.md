# Extending and Integrating Cruise Control

**Part 4 of 6** - Practical Guide to Customization

**Reading Time:** ~10 minutes | **Analysis Basis:** Commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

## What You'll Learn

- How to implement a custom goal from scratch
- Integrating with external monitoring systems (Prometheus, Datadog)
- Custom anomaly notifiers for Slack/PagerDuty
- REST API integration patterns

## Implementing a Custom Goal

Let's build `DailyPeakAwareGoal`—a goal that ensures brokers can handle daily traffic peaks.

### Step 1: Extend AbstractGoal

```java
package com.mycompany.cruisecontrol.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.AbstractGoal;
import com.linkedin.kafka.cruisecontrol.model.*;

public class DailyPeakAwareGoal extends AbstractGoal {
    
    private static final double PEAK_TRAFFIC_MULTIPLIER = 1.5;
    
    @Override
    public String name() {
        return DailyPeakAwareGoal.class.getSimpleName();
    }
    
    @Override
    protected void initGoalState(ClusterModel clusterModel, 
                                  OptimizationOptions options) {
        // Initialize: find brokers that can't handle peak
        _brokersToBalance = new HashSet<>();
        for (Broker broker : clusterModel.healthyBrokers()) {
            double currentLoad = broker.load().expectedUtilizationFor(Resource.NW_IN);
            double peakLoad = currentLoad * PEAK_TRAFFIC_MULTIPLIER;
            double capacity = broker.capacityFor(Resource.NW_IN);
            
            if (peakLoad > capacity * 0.8) {  // Would exceed 80% at peak
                _brokersToBalance.add(broker);
            }
        }
    }
    
    @Override
    protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
        return _brokersToBalance;
    }
    
    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, 
                                     BalancingAction action) {
        // Check if action maintains peak capacity
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        double newLoad = sourceBroker.load().expectedUtilizationFor(Resource.NW_IN)
                       - action.replica().load().expectedUtilizationFor(Resource.NW_IN);
        double peakLoad = newLoad * PEAK_TRAFFIC_MULTIPLIER;
        
        return peakLoad <= sourceBroker.capacityFor(Resource.NW_IN) * 0.8;
    }
    
    @Override
    protected void rebalanceForBroker(Broker broker, 
                                       ClusterModel clusterModel,
                                       Set<Goal> optimizedGoals, 
                                       OptimizationOptions options) {
        // Move replicas off overloaded broker
        List<Replica> replicasToMove = broker.replicas().stream()
            .sorted(Comparator.comparing(r -> r.load().expectedUtilizationFor(Resource.NW_IN))
                    .reversed())
            .collect(Collectors.toList());
        
        for (Replica replica : replicasToMove) {
            // Find destination broker with capacity
            for (Broker targetBroker : clusterModel.healthyBrokers()) {
                if (targetBroker.id() == broker.id()) continue;
                
                double targetPeakLoad = (targetBroker.load().expectedUtilizationFor(Resource.NW_IN)
                                        + replica.load().expectedUtilizationFor(Resource.NW_IN))
                                        * PEAK_TRAFFIC_MULTIPLIER;
                double targetCapacity = targetBroker.capacityFor(Resource.NW_IN);
                
                if (targetPeakLoad <= targetCapacity * 0.8) {
                    // Check with other goals
                    BalancingAction action = new BalancingAction(replica, targetBroker);
                    if (approvedByOtherGoals(action, optimizedGoals, clusterModel)) {
                        clusterModel.relocateReplica(replica, targetBroker);
                        break;
                    }
                }
            }
        }
    }
    
    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new GoalUtils.HardGoalStatsComparator();
    }
    
    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(1, 0.95, true);
    }
}
```

### Step 2: Register the Goal

In `cruisecontrol.properties`:

```properties
goals=com.mycompany.cruisecontrol.goals.DailyPeakAwareGoal,\
      com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,\
      ...

default.goals=com.mycompany.cruisecontrol.goals.DailyPeakAwareGoal,\
              ...
```

### Step 3: Test Your Goal

```java
@Test
public void testDailyPeakAwareGoal() {
    // Create test cluster
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel();
    
    // Add high-traffic replica to broker-0
    Broker broker0 = clusterModel.broker(0);
    Replica highTrafficReplica = broker0.replicas().iterator().next();
    highTrafficReplica.setLoad(createHighLoad());
    
    // Run goal
    DailyPeakAwareGoal goal = new DailyPeakAwareGoal();
    goal.configure(new HashMap<>());
    
    boolean violated = goal.optimize(clusterModel, new HashSet<>(), options);
    
    // Assert: broker-0 can now handle peak
    assertTrue(violated);
    double peakLoad = broker0.load().expectedUtilizationFor(Resource.NW_IN) * 1.5;
    double capacity = broker0.capacityFor(Resource.NW_IN);
    assertTrue(peakLoad <= capacity * 0.8);
}
```

## Custom Metric Sampler: Prometheus Integration

Instead of reading metrics from Kafka, read from Prometheus:

```java
package com.mycompany.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.cruisecontrol.monitor.sampling.Samples;
import io.prometheus.client.exporter.HTTPServer;

public class PrometheusMetricSampler implements MetricSampler {
    
    private String _prometheusUrl;
    private HttpClient _client;
    
    @Override
    public void configure(Map<String, ?> configs) {
        _prometheusUrl = (String) configs.get("prometheus.url");
        _client = HttpClient.newHttpClient();
    }
    
    @Override
    public Samples getSamples(MetricSamplerOptions options) 
            throws SamplingException {
        try {
            // Query Prometheus for broker metrics
            String query = "kafka_server_broker_topic_metrics_bytes_in_per_sec";
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(_prometheusUrl + "/api/v1/query?query=" + query))
                .build();
            
            HttpResponse<String> response = _client.send(request, 
                HttpResponse.BodyHandlers.ofString());
            
            // Parse Prometheus response
            JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonArray results = json.getAsJsonObject("data")
                                    .getAsJsonArray("result");
            
            // Convert to Cruise Control Samples
            Samples samples = new Samples();
            for (JsonElement result : results) {
                int brokerId = extractBrokerId(result);
                String topic = extractTopic(result);
                double value = extractValue(result);
                
                samples.addSample(new PartitionMetricSample(
                    brokerId,
                    new TopicPartition(topic, 0),
                    RawMetricType.TOPIC_BYTES_IN,
                    value,
                    System.currentTimeMillis()
                ));
            }
            
            return samples;
        } catch (IOException | InterruptedException e) {
            throw new SamplingException("Failed to query Prometheus", e);
        }
    }
    
    @Override
    public void close() {
        // Cleanup
    }
}
```

**Configuration:**

```properties
metric.sampler.class=com.mycompany.cruisecontrol.monitor.PrometheusMetricSampler
prometheus.url=http://prometheus:9090
```

## Custom Anomaly Notifier: Slack Integration

Send anomaly alerts to Slack:

```java
package com.mycompany.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.*;

public class SlackAnomalyNotifier implements AnomalyNotifier {
    
    private String _webhookUrl;
    private HttpClient _client;
    
    @Override
    public void configure(Map<String, ?> configs) {
        _webhookUrl = (String) configs.get("slack.webhook.url");
        _client = HttpClient.newHttpClient();
    }
    
    @Override
    public AnomalyNotificationResult onBrokerFailure(BrokerFailures failures) {
        String message = String.format(
            "🚨 *Broker Failure Detected*\n" +
            "Failed brokers: %s\n" +
            "Time: %s\n" +
            "Action: %s",
            failures.failedBrokers(),
            Instant.now(),
            "Auto-healing in 5 minutes"
        );
        
        sendToSlack(message);
        
        // Return CHECK to wait before auto-healing
        return AnomalyNotificationResult.check(Duration.ofMinutes(5));
    }
    
    @Override
    public AnomalyNotificationResult onGoalViolation(GoalViolations violations) {
        String message = String.format(
            "⚠️ *Goal Violations Detected*\n" +
            "Violated goals: %s\n" +
            "Severity: %s",
            violations.violatedGoalNames(),
            violations.isFixable() ? "Fixable" : "Not fixable"
        );
        
        sendToSlack(message);
        
        return violations.isFixable() 
            ? AnomalyNotificationResult.fix()
            : AnomalyNotificationResult.ignore();
    }
    
    private void sendToSlack(String message) {
        try {
            String payload = String.format("{\"text\":\"%s\"}", message);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(_webhookUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();
            
            _client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            LOG.error("Failed to send Slack notification", e);
        }
    }
}
```

**Configuration:**

```properties
anomaly.notifier.class=com.mycompany.cruisecontrol.detector.SlackAnomalyNotifier
slack.webhook.url=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

## REST API Integration Patterns

### Pattern 1: Programmatic Rebalance

```java
public class CruiseControlClient {
    
    private final String baseUrl;
    private final HttpClient client;
    
    public void triggerRebalance(List<String> goals) throws IOException {
        // Build request
        String goalsParam = String.join(",", goals);
        String url = baseUrl + "/kafkacruisecontrol/rebalance?goals=" + goalsParam 
                   + "&dryrun=false&json=true";
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        
        // Send request
        HttpResponse<String> response = client.send(request, 
            HttpResponse.BodyHandlers.ofString());
        
        // Parse response
        JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
        String userTaskId = json.get("userTaskId").getAsString();
        
        // Poll for completion
        pollTaskCompletion(userTaskId);
    }
    
    private void pollTaskCompletion(String taskId) throws IOException {
        while (true) {
            String url = baseUrl + "/kafkacruisecontrol/user_tasks?user_task_ids=" + taskId;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
            
            HttpResponse<String> response = client.send(request, 
                HttpResponse.BodyHandlers.ofString());
            
            JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonArray tasks = json.getAsJsonArray("userTasks");
            JsonObject task = tasks.get(0).getAsJsonObject();
            
            String status = task.get("status").getAsString();
            
            if (status.equals("Completed")) {
                LOG.info("Rebalance completed successfully");
                return;
            } else if (status.equals("CompletedWithError")) {
                throw new RuntimeException("Rebalance failed: " + task.get("error"));
            }
            
            // Wait 5 seconds before polling again
            Thread.sleep(5000);
        }
    }
}
```

### Pattern 2: Webhook Integration

React to Cruise Control events:

```java
@RestController
@RequestMapping("/cruise-control-webhooks")
public class CruiseControlWebhookController {
    
    @PostMapping("/broker-failure")
    public ResponseEntity<String> handleBrokerFailure(@RequestBody String payload) {
        JsonObject event = JsonParser.parseString(payload).getAsJsonObject();
        
        int brokerId = event.get("brokerId").getAsInt();
        long timestamp = event.get("timestamp").getAsLong();
        
        // Custom logic: maybe page on-call engineer
        pagerDutyService.alert("Broker " + brokerId + " failed at " + timestamp);
        
        // Maybe trigger custom remediation
        if (isBusinessHours()) {
            // Auto-heal during business hours
            cruiseControlClient.fixBrokerFailure(brokerId);
        } else {
            // Wait for human during off-hours
            LOG.warn("Broker failure detected off-hours, waiting for manual intervention");
        }
        
        return ResponseEntity.ok("Handled");
    }
}
```

## Key Takeaways

1. **Goals are your main extension point:** Implement custom optimization logic
2. **Custom samplers integrate with your monitoring:** Prometheus, Datadog, etc.
3. **Notifiers hook into your alerting:** Slack, PagerDuty, email
4. **REST API enables automation:** Build workflows around Cruise Control
5. **All extensions follow the same pattern:** Implement interface, configure class name

## Next Steps

1. **Implement a simple goal:** Start with a distribution goal
2. **Integrate with your monitoring:** Replace default sampler
3. **Set up notifications:** Connect to your team's Slack
4. **Next in series:** Post 5 covers performance and optimization opportunities

---

**Next Post:** [Performance Analysis and Optimization Opportunities →](./05-performance-analysis.md)

*Part 4 of 6 | All code references based on commit [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)*
