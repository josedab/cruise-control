/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import java.util.Collections;
import java.util.List;

/**
 * Specification for a partition to add in a simulation scenario.
 */
public class PartitionSpec {
  private final String _topic;
  private final int _partition;
  private final List<Integer> _replicas;

  /**
   * Constructor for PartitionSpec.
   *
   * @param topic topic name
   * @param partition partition number
   * @param replicas list of broker IDs for replicas
   */
  public PartitionSpec(String topic, int partition, List<Integer> replicas) {
    _topic = topic;
    _partition = partition;
    _replicas = Collections.unmodifiableList(replicas);
  }

  public String topic() {
    return _topic;
  }

  public int partition() {
    return _partition;
  }

  public List<Integer> replicas() {
    return _replicas;
  }

  @Override
  public String toString() {
    return String.format("PartitionSpec{topic='%s', partition=%d, replicas=%s}", _topic, _partition, _replicas);
  }
}
