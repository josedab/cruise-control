/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

/**
 * Represents the type of change that occurred in the cluster model.
 * This is used to determine whether cached proposals can be reused with partial updates
 * or need to be fully regenerated.
 *
 * <p>The change types are ordered from least to most disruptive:</p>
 * <ul>
 *   <li>{@link #METRICS_ONLY} - Only metric values changed, topology unchanged</li>
 *   <li>{@link #TOPIC_ADDED} - New topic(s) added (typically already balanced)</li>
 *   <li>{@link #PARTITION_ADDED} - Partition count increased for existing topic(s)</li>
 *   <li>{@link #SINGLE_BROKER_ADDED} - Exactly one broker joined the cluster</li>
 *   <li>{@link #SINGLE_BROKER_REMOVED} - Exactly one broker left the cluster</li>
 *   <li>{@link #MAJOR_TOPOLOGY_CHANGE} - Multiple brokers/topics changed, or other complex changes</li>
 * </ul>
 */
public enum ClusterChangeType {
  /**
   * Only metric values have changed (CPU, network, disk usage, etc.).
   * The cluster topology (brokers, topics, partitions) remains unchanged.
   * Cached proposals can be reused with updated estimates.
   */
  METRICS_ONLY,

  /**
   * One or more new topics have been added.
   * New topic partitions are typically already balanced across brokers,
   * so existing proposals often remain valid.
   */
  TOPIC_ADDED,

  /**
   * The partition count increased for one or more existing topics.
   * New partitions need to be assigned, but existing proposals may be partially reusable.
   */
  PARTITION_ADDED,

  /**
   * Exactly one broker was added to the cluster.
   * Only a subset of partitions need to be moved to balance load on the new broker.
   * Most cached proposals can be reused.
   */
  SINGLE_BROKER_ADDED,

  /**
   * Exactly one broker was removed from the cluster.
   * Only partitions from the removed broker need to be reassigned.
   * Some cached proposals can be reused.
   */
  SINGLE_BROKER_REMOVED,

  /**
   * Multiple brokers changed, multiple topics changed, or other complex topology changes.
   * This requires full proposal regeneration as cached proposals are likely invalid.
   */
  MAJOR_TOPOLOGY_CHANGE
}
