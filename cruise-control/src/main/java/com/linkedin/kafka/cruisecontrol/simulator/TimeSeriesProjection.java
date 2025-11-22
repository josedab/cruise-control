/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.Modifications;
import com.linkedin.kafka.cruisecontrol.simulator.model.Recommendation;
import com.linkedin.kafka.cruisecontrol.simulator.model.ResourceUtilization;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import com.linkedin.kafka.cruisecontrol.simulator.model.Severity;
import com.linkedin.kafka.cruisecontrol.simulator.model.SimulationReport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Projects cluster capacity needs over time.
 * <p>
 * This class simulates future growth scenarios to help with capacity planning.
 * </p>
 */
public class TimeSeriesProjection {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesProjection.class);

  private final SimulationExecutor _executor;

  /**
   * Constructor for TimeSeriesProjection.
   *
   * @param executor simulation executor
   */
  public TimeSeriesProjection(SimulationExecutor executor) {
    _executor = executor;
  }

  /**
   * Projects capacity needs over multiple time periods.
   *
   * @param projections list of projection configurations
   * @param capacityTarget target capacity headroom
   * @param goals optimization goals
   * @param requirements model completeness requirements
   * @return projection results
   * @throws SimulationException if projection fails
   */
  public ProjectionResult project(List<ProjectionConfig> projections,
                                  CapacityTarget capacityTarget,
                                  List<Goal> goals,
                                  ModelCompletenessRequirements requirements) throws SimulationException {
    LOG.info("Running time-series projection for {} time periods", projections.size());

    List<ProjectionPoint> points = new ArrayList<>();

    for (ProjectionConfig config : projections) {
      try {
        ProjectionPoint point = projectTimePoint(config, capacityTarget, goals, requirements);
        points.add(point);
        LOG.debug("Projection for {} months: {} brokers needed",
            config.months(), point.brokersNeeded());
      } catch (SimulationException e) {
        LOG.warn("Projection for {} months failed: {}", config.months(), e.getMessage());
      }
    }

    // Generate overall recommendations
    List<Recommendation> recommendations = generateRecommendations(points, projections);

    return new ProjectionResult(points, recommendations);
  }

  /**
   * Projects capacity needs for a single time point.
   */
  private ProjectionPoint projectTimePoint(ProjectionConfig config,
                                           CapacityTarget target,
                                           List<Goal> goals,
                                           ModelCompletenessRequirements requirements) throws SimulationException {
    // Create scenario with load growth
    Map<String, Double> loadMultipliers = new HashMap<>();
    loadMultipliers.put("ALL", config.loadGrowth());

    Modifications modifications = new Modifications(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        loadMultipliers
    );

    Scenario scenario = new Scenario(
        String.format("Projection %d months (%.1fx load)", config.months(), config.loadGrowth()),
        modifications
    );

    // Run simulation
    SimulationReport report = _executor.execute(scenario, goals, requirements);

    // Calculate brokers needed to meet target
    int brokersNeeded = calculateBrokersNeeded(report, target);
    int currentBrokers = report.clusterStats().brokers();
    int additionalBrokers = Math.max(0, brokersNeeded - currentBrokers);

    // Estimate cost impact (simplified - would need real cost data)
    double costPerBroker = 500.0; // Placeholder monthly cost
    double costImpact = additionalBrokers * costPerBroker;

    String action = additionalBrokers > 0
        ? String.format("Add %d brokers", additionalBrokers)
        : "No action needed";

    return new ProjectionPoint(
        config.months(),
        config.loadGrowth(),
        brokersNeeded,
        additionalBrokers,
        costImpact,
        action
    );
  }

  /**
   * Calculates brokers needed to meet capacity target.
   */
  private int calculateBrokersNeeded(SimulationReport report, CapacityTarget target) {
    int currentBrokers = report.clusterStats().brokers();
    double maxUtilization = 0;

    CapacityAnalysis capacity = report.capacityAnalysis();
    for (ResourceUtilization util : capacity.utilization().values()) {
      maxUtilization = Math.max(maxUtilization, util.max());
    }

    // Calculate how many brokers needed to achieve target headroom
    double targetMaxUtilization = 100.0 - target.headroom();
    if (maxUtilization <= targetMaxUtilization) {
      return currentBrokers;
    }

    // Simple linear scaling calculation
    double scaleFactor = maxUtilization / targetMaxUtilization;
    return (int) Math.ceil(currentBrokers * scaleFactor);
  }

  /**
   * Generates recommendations based on projections.
   */
  private List<Recommendation> generateRecommendations(List<ProjectionPoint> points,
                                                       List<ProjectionConfig> configs) {
    List<Recommendation> recommendations = new ArrayList<>();

    if (points.isEmpty()) {
      recommendations.add(new Recommendation(
          Severity.LOW,
          "No projection data",
          "Could not generate projections - check cluster health"
      ));
      return recommendations;
    }

    // Find the 12-month projection if available
    ProjectionPoint longTerm = points.stream()
        .filter(p -> p.months() >= 12)
        .findFirst()
        .orElse(points.get(points.size() - 1));

    if (longTerm.additionalBrokers() > 0) {
      recommendations.add(new Recommendation(
          Severity.MEDIUM,
          "Plan capacity expansion",
          String.format("Plan hardware procurement for %d additional brokers " +
                  "within %d months (estimated cost: $%.0f/month)",
              longTerm.additionalBrokers(), longTerm.months(), longTerm.costImpact())
      ));
    }

    // Check if growth is aggressive
    double maxGrowth = configs.stream()
        .mapToDouble(ProjectionConfig::loadGrowth)
        .max()
        .orElse(1.0);

    if (maxGrowth > 2.0) {
      recommendations.add(new Recommendation(
          Severity.HIGH,
          "High growth anticipated",
          String.format("%.0fx growth projected - consider larger instance types to reduce broker count",
              maxGrowth)
      ));
    }

    // Check if any projection shows insufficient capacity
    boolean hasInsufficientCapacity = points.stream()
        .anyMatch(p -> p.additionalBrokers() > p.brokersNeeded() / 2);

    if (hasInsufficientCapacity) {
      recommendations.add(new Recommendation(
          Severity.HIGH,
          "Significant capacity gap",
          "Projected growth may exceed available capacity - start procurement now"
      ));
    }

    if (recommendations.isEmpty()) {
      recommendations.add(new Recommendation(
          Severity.INFO,
          "Capacity planning on track",
          "Current capacity can handle projected growth with minor adjustments"
      ));
    }

    return recommendations;
  }

  /**
   * Configuration for a time projection point.
   */
  public static class ProjectionConfig {
    private final int _months;
    private final double _loadGrowth;

    public ProjectionConfig(int months, double loadGrowth) {
      _months = months;
      _loadGrowth = loadGrowth;
    }

    public int months() {
      return _months;
    }

    public double loadGrowth() {
      return _loadGrowth;
    }
  }

  /**
   * Target capacity configuration.
   */
  public static class CapacityTarget {
    private final double _headroom;

    public CapacityTarget(double headroom) {
      _headroom = headroom;
    }

    public double headroom() {
      return _headroom;
    }
  }

  /**
   * Result of a single projection point.
   */
  public static class ProjectionPoint {
    private final int _months;
    private final double _loadGrowth;
    private final int _brokersNeeded;
    private final int _additionalBrokers;
    private final double _costImpact;
    private final String _action;

    public ProjectionPoint(int months, double loadGrowth, int brokersNeeded,
                          int additionalBrokers, double costImpact, String action) {
      _months = months;
      _loadGrowth = loadGrowth;
      _brokersNeeded = brokersNeeded;
      _additionalBrokers = additionalBrokers;
      _costImpact = costImpact;
      _action = action;
    }

    public int months() {
      return _months;
    }

    public double loadGrowth() {
      return _loadGrowth;
    }

    public int brokersNeeded() {
      return _brokersNeeded;
    }

    public int additionalBrokers() {
      return _additionalBrokers;
    }

    public double costImpact() {
      return _costImpact;
    }

    public String action() {
      return _action;
    }
  }

  /**
   * Complete projection result.
   */
  public static class ProjectionResult {
    private final List<ProjectionPoint> _projections;
    private final List<Recommendation> _recommendations;

    public ProjectionResult(List<ProjectionPoint> projections, List<Recommendation> recommendations) {
      _projections = projections;
      _recommendations = recommendations;
    }

    public List<ProjectionPoint> projections() {
      return _projections;
    }

    public List<Recommendation> recommendations() {
      return _recommendations;
    }
  }
}
