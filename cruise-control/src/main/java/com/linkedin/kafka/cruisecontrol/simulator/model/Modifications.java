/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Modifications to apply to a cluster in a simulation scenario.
 */
public class Modifications {
  private final List<BrokerSpec> _additionalBrokers;
  private final List<Integer> _removedBrokers;
  private final List<PartitionSpec> _additionalPartitions;
  private final Map<String, Double> _loadMultipliers;

  /**
   * Constructor for Modifications.
   *
   * @param additionalBrokers brokers to add
   * @param removedBrokers broker IDs to remove
   * @param additionalPartitions partitions to add
   * @param loadMultipliers load multipliers by resource type
   */
  public Modifications(List<BrokerSpec> additionalBrokers, List<Integer> removedBrokers,
                       List<PartitionSpec> additionalPartitions, Map<String, Double> loadMultipliers) {
    _additionalBrokers = additionalBrokers != null ? Collections.unmodifiableList(additionalBrokers) : Collections.emptyList();
    _removedBrokers = removedBrokers != null ? Collections.unmodifiableList(removedBrokers) : Collections.emptyList();
    _additionalPartitions = additionalPartitions != null ? Collections.unmodifiableList(additionalPartitions) : Collections.emptyList();
    _loadMultipliers = loadMultipliers != null ? Collections.unmodifiableMap(loadMultipliers) : Collections.emptyMap();
  }

  /**
   * Empty modifications constructor.
   */
  public Modifications() {
    this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
  }

  public List<BrokerSpec> additionalBrokers() {
    return _additionalBrokers;
  }

  public List<Integer> removedBrokers() {
    return _removedBrokers;
  }

  public List<PartitionSpec> additionalPartitions() {
    return _additionalPartitions;
  }

  public Map<String, Double> loadMultipliers() {
    return _loadMultipliers;
  }

  @Override
  public String toString() {
    return String.format("Modifications{additionalBrokers=%d, removedBrokers=%d, additionalPartitions=%d, loadMultipliers=%s}",
        _additionalBrokers.size(), _removedBrokers.size(), _additionalPartitions.size(), _loadMultipliers);
  }
}
