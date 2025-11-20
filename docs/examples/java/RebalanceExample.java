package com.linkedin.kafka.cruisecontrol.examples;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Example: Programmatic rebalance using Cruise Control REST API.
 *
 * <p>This example demonstrates how to:
 * <ul>
 *   <li>Request a cluster rebalance with specific goals</li>
 *   <li>Poll for execution completion</li>
 *   <li>Handle errors gracefully</li>
 * </ul>
 *
 * <p><b>Prerequisites:</b>
 * <ul>
 *   <li>Cruise Control running on localhost:9090</li>
 *   <li>Kafka cluster with metrics configured</li>
 *   <li>At least 1 hour of metrics collected</li>
 * </ul>
 *
 * <p><b>Dependencies:</b>
 * <pre>
 * // Add to pom.xml:
 * &lt;dependency&gt;
 *   &lt;groupId&gt;com.fasterxml.jackson.core&lt;/groupId&gt;
 *   &lt;artifactId&gt;jackson-databind&lt;/artifactId&gt;
 *   &lt;version&gt;2.13.0&lt;/version&gt;
 * &lt;/dependency&gt;
 * </pre>
 *
 * <p><b>Usage:</b>
 * <pre>
 * javac RebalanceExample.java
 * java RebalanceExample
 * </pre>
 */
public class RebalanceExample {
  private static final String CRUISE_CONTROL_URL = "http://localhost:9090/kafkacruisecontrol";
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    System.out.println("=== Cruise Control Rebalance Example ===\n");

    // Step 1: Request rebalance
    System.out.println("Step 1: Requesting rebalance...");
    String userTaskId = requestRebalance();
    System.out.println("✓ Rebalance requested. User Task ID: " + userTaskId + "\n");

    // Step 2: Poll for completion
    System.out.println("Step 2: Monitoring execution progress...");
    monitorExecution(userTaskId);

    System.out.println("\n=== Rebalance Complete ===");
  }

  /**
   * Requests a cluster rebalance with default goals.
   *
   * @return the user task ID for tracking execution
   * @throws Exception if the request fails
   */
  private static String requestRebalance() throws Exception {
    // Build goals list (using most important goals)
    String goals = String.join(",",
        "RackAwareGoal",
        "ReplicaCapacityGoal",
        "DiskCapacityGoal",
        "NetworkInboundCapacityGoal",
        "NetworkOutboundCapacityGoal",
        "CpuCapacityGoal",
        "ReplicaDistributionGoal",
        "DiskUsageDistributionGoal",
        "NetworkInboundUsageDistributionGoal",
        "NetworkOutboundUsageDistributionGoal",
        "CpuUsageDistributionGoal"
    );

    // Build request URL
    String url = String.format("%s/rebalance?goals=%s&dryrun=false&verbose=true&json=true",
        CRUISE_CONTROL_URL, goals);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .POST(HttpRequest.BodyPublishers.noBody())
        .header("Content-Type", "application/json")
        .build();

    HttpResponse<String> response = HTTP_CLIENT.send(request,
        HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException("Rebalance request failed: HTTP " + response.statusCode()
          + "\n" + response.body());
    }

    // Parse user task ID from response
    JsonNode json = OBJECT_MAPPER.readTree(response.body());
    return json.get("userTaskId").asText();
  }

  /**
   * Monitors execution progress by polling the user_tasks endpoint.
   *
   * @param userTaskId the user task ID to monitor
   * @throws Exception if polling fails
   */
  private static void monitorExecution(String userTaskId) throws Exception {
    int pollCount = 0;
    long startTime = System.currentTimeMillis();

    while (true) {
      pollCount++;

      // Query task status
      UserTaskState state = getUserTaskState(userTaskId);

      // Calculate elapsed time
      long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;

      // Print progress
      System.out.printf("[%02d:%02d] Poll #%d: %s%n",
          elapsedSeconds / 60,
          elapsedSeconds % 60,
          pollCount,
          state.status());

      // Check if completed
      if ("Completed".equals(state.status())) {
        System.out.println("\n✓ Rebalance completed successfully!");
        if (state.summary() != null) {
          System.out.println("Summary: " + state.summary());
        }
        break;
      } else if ("CompletedWithError".equals(state.status())) {
        System.err.println("\n✗ Rebalance failed!");
        if (state.error() != null) {
          System.err.println("Error: " + state.error());
        }
        throw new RuntimeException("Rebalance failed");
      }

      // Wait 5 seconds before next poll
      Thread.sleep(5000);
    }
  }

  /**
   * Gets the current state of a user task.
   *
   * @param userTaskId the user task ID
   * @return the user task state
   * @throws Exception if the request fails
   */
  private static UserTaskState getUserTaskState(String userTaskId) throws Exception {
    String url = String.format("%s/user_tasks?user_task_ids=%s&json=true",
        CRUISE_CONTROL_URL, userTaskId);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .GET()
        .build();

    HttpResponse<String> response = HTTP_CLIENT.send(request,
        HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException("Failed to get task status: HTTP " + response.statusCode());
    }

    JsonNode json = OBJECT_MAPPER.readTree(response.body());
    JsonNode userTasks = json.get("userTasks");

    if (userTasks == null || userTasks.size() == 0) {
      throw new RuntimeException("User task not found: " + userTaskId);
    }

    JsonNode task = userTasks.get(0);

    return new UserTaskState(
        task.get("Status").asText(),
        task.has("Summary") ? task.get("Summary").asText() : null,
        task.has("Error") ? task.get("Error").asText() : null
    );
  }

  /**
   * Represents the state of a user task.
   *
   * @param status the task status (Active, InExecution, Completed, CompletedWithError)
   * @param summary optional summary of the task result
   * @param error optional error message if task failed
   */
  private record UserTaskState(String status, String summary, String error) {}
}
