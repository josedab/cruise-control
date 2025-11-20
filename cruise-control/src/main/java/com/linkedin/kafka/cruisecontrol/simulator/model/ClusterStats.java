/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * Basic statistics about a cluster.
 */
public class ClusterStats {
  private final int _brokers;
  private final int _partitions;
  private final int _replicas;
  private final long _totalDataBytes;

  /**
   * Constructor for ClusterStats.
   *
   * @param brokers number of brokers
   * @param partitions number of partitions
   * @param replicas number of replicas
   * @param totalDataBytes total data in bytes
   */
  public ClusterStats(int brokers, int partitions, int replicas, long totalDataBytes) {
    _brokers = brokers;
    _partitions = partitions;
    _replicas = replicas;
    _totalDataBytes = totalDataBytes;
  }

  public int brokers() {
    return _brokers;
  }

  public int partitions() {
    return _partitions;
  }

  public int replicas() {
    return _replicas;
  }

  public long totalDataBytes() {
    return _totalDataBytes;
  }

  @Override
  public String toString() {
    return String.format("ClusterStats{brokers=%d, partitions=%d, replicas=%d, totalDataBytes=%d}",
        _brokers, _partitions, _replicas, _totalDataBytes);
  }
}
