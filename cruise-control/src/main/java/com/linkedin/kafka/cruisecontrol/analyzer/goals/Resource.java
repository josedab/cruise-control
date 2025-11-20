/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

/**
 * Enumeration of resources that goals can modify during optimization.
 * Used by {@link GoalDependencyAnalyzer} to determine if goals can run in parallel.
 *
 * Goals that modify disjoint sets of resources can potentially run in parallel,
 * while goals that modify overlapping resources must run sequentially.
 */
public enum Resource {
  /**
   * Rack placement of replicas (rack-aware distribution)
   */
  RACK_DISTRIBUTION,

  /**
   * CPU capacity utilization across brokers
   */
  CPU_CAPACITY,

  /**
   * Disk capacity utilization across brokers
   */
  DISK_CAPACITY,

  /**
   * Network inbound capacity utilization
   */
  NETWORK_INBOUND_CAPACITY,

  /**
   * Network outbound capacity utilization
   */
  NETWORK_OUTBOUND_CAPACITY,

  /**
   * Distribution of replicas across brokers
   */
  REPLICA_DISTRIBUTION,

  /**
   * Distribution of CPU usage across brokers
   */
  CPU_USAGE_DISTRIBUTION,

  /**
   * Distribution of disk usage across brokers
   */
  DISK_USAGE_DISTRIBUTION,

  /**
   * Distribution of network inbound usage across brokers
   */
  NETWORK_INBOUND_USAGE_DISTRIBUTION,

  /**
   * Distribution of network outbound usage across brokers
   */
  NETWORK_OUTBOUND_USAGE_DISTRIBUTION,

  /**
   * Distribution of topic replicas across brokers
   */
  TOPIC_REPLICA_DISTRIBUTION,

  /**
   * Distribution of leader replicas across brokers
   */
  LEADER_REPLICA_DISTRIBUTION,

  /**
   * Distribution of leader bytes-in across brokers
   */
  LEADER_BYTES_IN_DISTRIBUTION,

  /**
   * Preferred leader election (leadership placement)
   */
  PREFERRED_LEADER_ELECTION,

  /**
   * Potential network out capacity (network topology aware)
   */
  POTENTIAL_NETWORK_OUT,

  /**
   * Minimum topic leader replica distribution
   */
  MIN_TOPIC_LEADERS_PER_BROKER,

  /**
   * Intra-broker disk capacity balancing
   */
  INTRA_BROKER_DISK_CAPACITY,

  /**
   * Intra-broker disk usage distribution
   */
  INTRA_BROKER_DISK_USAGE_DISTRIBUTION
}
