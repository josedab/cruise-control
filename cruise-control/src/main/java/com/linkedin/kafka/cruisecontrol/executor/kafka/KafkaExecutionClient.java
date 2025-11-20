/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.kafka;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.metrics.ExecutionMetrics;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ElectionType;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes operations against Kafka cluster via AdminClient.
 *
 * <p>This class is the sole interface to Kafka AdminClient for execution
 * operations, making it easy to mock for testing.
 */
public class KafkaExecutionClient {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaExecutionClient.class);
  private static final long DEFAULT_TIMEOUT_MS = 30000; // 30 seconds

  private final AdminClient _adminClient;
  private final ExecutionMetrics _metrics;

  /**
   * Creates a new KafkaExecutionClient.
   *
   * @param adminClient the Kafka AdminClient to use
   * @param metrics the metrics tracker
   */
  public KafkaExecutionClient(AdminClient adminClient, ExecutionMetrics metrics) {
    _adminClient = adminClient;
    _metrics = metrics;
  }

  /**
   * Submits partition reassignment requests to Kafka.
   *
   * @param reassignments the partition reassignments to execute
   * @return future that completes when submission is done (not execution)
   */
  public CompletableFuture<Void> submitReassignments(
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {

    _metrics.recordApiCall("alterPartitionReassignments");

    return CompletableFuture.supplyAsync(() -> {
      try {
        AlterPartitionReassignmentsResult result =
            _adminClient.alterPartitionReassignments(reassignments);

        // Wait for submission to complete
        result.all().get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        _metrics.recordApiSuccess("alterPartitionReassignments");
        LOG.debug("Successfully submitted {} partition reassignments", reassignments.size());
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        _metrics.recordApiFailure("alterPartitionReassignments");
        throw new KafkaExecutionException("Interrupted while submitting reassignments", e);
      } catch (ExecutionException | TimeoutException e) {
        _metrics.recordApiFailure("alterPartitionReassignments");
        throw new KafkaExecutionException("Failed to submit reassignments", e);
      }
    });
  }

  /**
   * Triggers leader election for specified partitions.
   *
   * @param partitions the partitions needing leader election
   * @return future that completes when elections finish
   */
  public CompletableFuture<Void> electLeaders(Collection<TopicPartition> partitions) {
    _metrics.recordApiCall("electLeaders");

    return CompletableFuture.supplyAsync(() -> {
      try {
        ElectLeadersResult result = _adminClient.electLeaders(
            ElectionType.PREFERRED,
            new HashSet<>(partitions)
        );

        result.all().get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        _metrics.recordApiSuccess("electLeaders");
        LOG.debug("Successfully elected leaders for {} partitions", partitions.size());
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        _metrics.recordApiFailure("electLeaders");
        throw new KafkaExecutionException("Interrupted while electing leaders", e);
      } catch (ExecutionException | TimeoutException e) {
        _metrics.recordApiFailure("electLeaders");
        throw new KafkaExecutionException("Failed to elect leaders", e);
      }
    });
  }

  /**
   * Queries current partition reassignment status.
   *
   * @param partitions partitions to check
   * @return map of partition to reassignment status
   */
  public Map<TopicPartition, PartitionReassignmentStatus> getReassignmentStatus(
      Collection<TopicPartition> partitions) {

    _metrics.recordApiCall("listPartitionReassignments");

    try {
      ListPartitionReassignmentsResult result =
          _adminClient.listPartitionReassignments(new HashSet<>(partitions));

      Map<TopicPartition, PartitionReassignment> ongoing =
          result.reassignments().get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      _metrics.recordApiSuccess("listPartitionReassignments");

      return convertToStatus(partitions, ongoing);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      _metrics.recordApiFailure("listPartitionReassignments");
      throw new KafkaExecutionException("Interrupted while checking reassignment status", e);
    } catch (ExecutionException | TimeoutException e) {
      _metrics.recordApiFailure("listPartitionReassignments");
      throw new KafkaExecutionException("Failed to check reassignment status", e);
    }
  }

  /**
   * Converts Kafka reassignment response to our internal status representation.
   */
  private Map<TopicPartition, PartitionReassignmentStatus> convertToStatus(
      Collection<TopicPartition> requested,
      Map<TopicPartition, PartitionReassignment> ongoing) {

    Map<TopicPartition, PartitionReassignmentStatus> result = new java.util.HashMap<>();

    for (TopicPartition tp : requested) {
      if (ongoing.containsKey(tp)) {
        // Reassignment is still in progress
        PartitionReassignment reassignment = ongoing.get(tp);
        result.put(tp, new PartitionReassignmentStatus(
            false, // not complete
            reassignment.replicas(),
            reassignment.addingReplicas(),
            reassignment.removingReplicas()
        ));
      } else {
        // No ongoing reassignment - either complete or not started
        result.put(tp, new PartitionReassignmentStatus(
            true, // complete
            null,
            null,
            null
        ));
      }
    }

    return result;
  }

  /**
   * Cancels ongoing reassignments for the specified partitions.
   *
   * @param partitions the partitions to cancel reassignment for
   * @return future that completes when cancellation is done
   */
  public CompletableFuture<Void> cancelReassignments(Set<TopicPartition> partitions) {
    _metrics.recordApiCall("cancelReassignments");

    return CompletableFuture.supplyAsync(() -> {
      try {
        // To cancel a reassignment, we send an empty reassignment
        Map<TopicPartition, Optional<NewPartitionReassignment>> cancellations =
            new java.util.HashMap<>();
        for (TopicPartition tp : partitions) {
          cancellations.put(tp, Optional.empty());
        }

        AlterPartitionReassignmentsResult result =
            _adminClient.alterPartitionReassignments(cancellations);

        result.all().get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        _metrics.recordApiSuccess("cancelReassignments");
        LOG.debug("Successfully cancelled reassignments for {} partitions", partitions.size());
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        _metrics.recordApiFailure("cancelReassignments");
        throw new KafkaExecutionException("Interrupted while cancelling reassignments", e);
      } catch (ExecutionException | TimeoutException e) {
        _metrics.recordApiFailure("cancelReassignments");
        throw new KafkaExecutionException("Failed to cancel reassignments", e);
      }
    });
  }

  /**
   * Gets the underlying AdminClient.
   * This is provided for operations that aren't yet abstracted.
   *
   * @return the admin client
   */
  public AdminClient getAdminClient() {
    return _adminClient;
  }

  /**
   * Closes the client and releases resources.
   */
  public void close() {
    if (_adminClient != null) {
      _adminClient.close();
    }
  }
}
