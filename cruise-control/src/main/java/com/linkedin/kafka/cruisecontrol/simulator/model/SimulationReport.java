/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import java.util.Collections;
import java.util.List;

/**
 * Complete report of a simulation execution.
 */
public class SimulationReport {
  private final String _scenarioName;
  private final ClusterStats _clusterStats;
  private final OptimizerResult _optimizationResult;
  private final CapacityAnalysis _capacityAnalysis;
  private final ImpactAssessment _impactAssessment;
  private final List<Recommendation> _recommendations;

  /**
   * Constructor for SimulationReport.
   *
   * @param scenarioName name of the scenario
   * @param clusterStats cluster statistics
   * @param optimizationResult optimization result
   * @param capacityAnalysis capacity analysis
   * @param impactAssessment impact assessment
   * @param recommendations list of recommendations
   */
  public SimulationReport(String scenarioName, ClusterStats clusterStats, OptimizerResult optimizationResult,
                          CapacityAnalysis capacityAnalysis, ImpactAssessment impactAssessment,
                          List<Recommendation> recommendations) {
    _scenarioName = scenarioName;
    _clusterStats = clusterStats;
    _optimizationResult = optimizationResult;
    _capacityAnalysis = capacityAnalysis;
    _impactAssessment = impactAssessment;
    _recommendations = recommendations != null ? Collections.unmodifiableList(recommendations) : Collections.emptyList();
  }

  public String scenarioName() {
    return _scenarioName;
  }

  public ClusterStats clusterStats() {
    return _clusterStats;
  }

  public OptimizerResult optimizationResult() {
    return _optimizationResult;
  }

  public CapacityAnalysis capacityAnalysis() {
    return _capacityAnalysis;
  }

  public ImpactAssessment impactAssessment() {
    return _impactAssessment;
  }

  public List<Recommendation> recommendations() {
    return _recommendations;
  }

  @Override
  public String toString() {
    return String.format("SimulationReport{scenarioName='%s', clusterStats=%s, recommendations=%d}",
        _scenarioName, _clusterStats, _recommendations.size());
  }
}
