/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import java.util.Collections;
import java.util.List;

/**
 * Assessment of the impact of proposed changes in a simulation.
 */
public class ImpactAssessment {
  private final int _partitionMovements;
  private final long _dataToMoveBytes;
  private final long _estimatedDurationMs;
  private final double _cpuImpact;
  private final double _networkImpact;
  private final List<String> _risks;

  /**
   * Constructor for ImpactAssessment.
   *
   * @param partitionMovements number of partition movements required
   * @param dataToMoveBytes amount of data to move in bytes
   * @param estimatedDurationMs estimated time to complete in milliseconds
   * @param cpuImpact CPU impact during execution (percentage)
   * @param networkImpact network impact during execution (percentage)
   * @param risks list of identified risks
   */
  public ImpactAssessment(int partitionMovements, long dataToMoveBytes, long estimatedDurationMs,
                          double cpuImpact, double networkImpact, List<String> risks) {
    _partitionMovements = partitionMovements;
    _dataToMoveBytes = dataToMoveBytes;
    _estimatedDurationMs = estimatedDurationMs;
    _cpuImpact = cpuImpact;
    _networkImpact = networkImpact;
    _risks = Collections.unmodifiableList(risks);
  }

  public int partitionMovements() {
    return _partitionMovements;
  }

  public long dataToMoveBytes() {
    return _dataToMoveBytes;
  }

  public long estimatedDurationMs() {
    return _estimatedDurationMs;
  }

  public double cpuImpact() {
    return _cpuImpact;
  }

  public double networkImpact() {
    return _networkImpact;
  }

  public List<String> risks() {
    return _risks;
  }

  @Override
  public String toString() {
    return String.format("ImpactAssessment{partitionMovements=%d, dataToMoveBytes=%d, estimatedDurationMs=%d, " +
            "cpuImpact=%.2f%%, networkImpact=%.2f%%, risks=%s}",
        _partitionMovements, _dataToMoveBytes, _estimatedDurationMs, _cpuImpact, _networkImpact, _risks);
  }
}
