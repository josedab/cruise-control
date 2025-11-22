/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.simulator.model.Assessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.Failure;
import com.linkedin.kafka.cruisecontrol.simulator.model.FailureType;
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
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monte Carlo simulation for probabilistic failure analysis.
 * <p>
 * This class runs multiple simulations with randomized failure scenarios
 * to understand the probability distribution of outcomes.
 * </p>
 */
public class MonteCarloSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(MonteCarloSimulator.class);

  private final SimulationExecutor _executor;
  private final LoadMonitor _loadMonitor;
  private final Random _random;

  /**
   * Constructor for MonteCarloSimulator.
   *
   * @param executor simulation executor
   * @param loadMonitor load monitor
   */
  public MonteCarloSimulator(SimulationExecutor executor, LoadMonitor loadMonitor) {
    this(executor, loadMonitor, new Random());
  }

  /**
   * Constructor with custom random source (for testing).
   *
   * @param executor simulation executor
   * @param loadMonitor load monitor
   * @param random random number generator
   */
  public MonteCarloSimulator(SimulationExecutor executor, LoadMonitor loadMonitor, Random random) {
    _executor = executor;
    _loadMonitor = loadMonitor;
    _random = random;
  }

  /**
   * Runs Monte Carlo simulation with specified parameters.
   *
   * @param config simulation configuration
   * @param goals optimization goals
   * @param requirements model completeness requirements
   * @return Monte Carlo simulation results
   * @throws SimulationException if simulation fails
   */
  public MonteCarloResult runSimulation(MonteCarloConfig config,
                                        List<Goal> goals,
                                        ModelCompletenessRequirements requirements) throws SimulationException {
    LOG.info("Starting Monte Carlo simulation with {} iterations", config.iterations());

    List<SimulationReport> reports = new ArrayList<>();
    int successCount = 0;
    int clusterHealthyCount = 0;
    int capacityExceededCount = 0;
    int dataLossRiskCount = 0;

    for (int i = 0; i < config.iterations(); i++) {
      try {
        // Generate random scenario based on config
        Scenario scenario = generateRandomScenario(config, i);

        // Execute simulation
        SimulationReport report = _executor.execute(scenario, goals, requirements);
        reports.add(report);
        successCount++;

        // Analyze result
        if (isClusterHealthy(report)) {
          clusterHealthyCount++;
        }
        if (isCapacityExceeded(report)) {
          capacityExceededCount++;
        }
        if (hasDataLossRisk(report)) {
          dataLossRiskCount++;
        }

        if ((i + 1) % 100 == 0) {
          LOG.debug("Completed {} of {} iterations", i + 1, config.iterations());
        }

      } catch (SimulationException e) {
        LOG.warn("Iteration {} failed: {}", i, e.getMessage());
        // Continue with other iterations
      }
    }

    // Calculate probabilities
    double healthyProbability = successCount > 0 ? (double) clusterHealthyCount / successCount : 0;
    double capacityExceededProbability = successCount > 0 ? (double) capacityExceededCount / successCount : 0;
    double dataLossProbability = successCount > 0 ? (double) dataLossRiskCount / successCount : 0;

    // Generate recommendations
    List<Recommendation> recommendations = generateRecommendations(
        healthyProbability, capacityExceededProbability, dataLossProbability);

    LOG.info("Monte Carlo simulation complete: {} successful iterations", successCount);

    return new MonteCarloResult(
        config.iterations(),
        successCount,
        healthyProbability,
        capacityExceededProbability,
        dataLossProbability,
        recommendations
    );
  }

  /**
   * Generates a random scenario based on configuration.
   */
  private Scenario generateRandomScenario(MonteCarloConfig config, int iteration) {
    List<Failure> failures = new ArrayList<>();

    // Simulate broker failures based on failure rate
    if (config.brokerFailureRate() > 0) {
      int brokerCount = getBrokerCount();
      for (int brokerId = 0; brokerId < brokerCount; brokerId++) {
        if (_random.nextDouble() < config.brokerFailureRate()) {
          failures.add(new Failure(FailureType.BROKER_DEAD, brokerId));
        }
      }
    }

    // Apply load variance
    Map<String, Double> loadMultipliers = new HashMap<>();
    if (config.loadVarianceMean() != 1.0 || config.loadVarianceStdDev() > 0) {
      double loadMultiplier = config.loadVarianceMean() +
          _random.nextGaussian() * config.loadVarianceStdDev();
      loadMultiplier = Math.max(0.1, loadMultiplier); // Ensure positive
      loadMultipliers.put("ALL", loadMultiplier);
    }

    Modifications modifications = new Modifications(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        loadMultipliers
    );

    return new Scenario(
        String.format("Monte Carlo Iteration %d", iteration),
        modifications,
        failures
    );
  }

  /**
   * Gets the current broker count from load monitor.
   */
  private int getBrokerCount() {
    try {
      ClusterModel model = _loadMonitor.clusterModel(
          System.currentTimeMillis(),
          null,
          null
      );
      return model.brokers().size();
    } catch (Exception e) {
      LOG.warn("Could not get broker count, using default of 10");
      return 10;
    }
  }

  /**
   * Checks if the cluster is healthy based on simulation report.
   */
  private boolean isClusterHealthy(SimulationReport report) {
    CapacityAnalysis capacity = report.capacityAnalysis();
    for (ResourceUtilization util : capacity.utilization().values()) {
      if (util.assessment() == Assessment.INSUFFICIENT) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if capacity was exceeded in the simulation.
   */
  private boolean isCapacityExceeded(SimulationReport report) {
    CapacityAnalysis capacity = report.capacityAnalysis();
    for (ResourceUtilization util : capacity.utilization().values()) {
      if (util.max() > 100) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if there's data loss risk based on simulation report.
   */
  private boolean hasDataLossRisk(SimulationReport report) {
    // Check if many brokers are dead relative to replication factor
    // This is a simplified check - in production would be more sophisticated
    return report.impactAssessment().risks().stream()
        .anyMatch(r -> r.toLowerCase().contains("data loss") || r.toLowerCase().contains("under-replicated"));
  }

  /**
   * Generates recommendations based on Monte Carlo results.
   */
  private List<Recommendation> generateRecommendations(double healthyProbability,
                                                        double capacityExceededProbability,
                                                        double dataLossProbability) {
    List<Recommendation> recommendations = new ArrayList<>();

    if (healthyProbability >= 0.95) {
      recommendations.add(new Recommendation(
          Severity.INFO,
          "Cluster is resilient",
          String.format("%.1f%% of simulations had healthy cluster - good failure tolerance",
              healthyProbability * 100)
      ));
    } else if (healthyProbability >= 0.80) {
      recommendations.add(new Recommendation(
          Severity.LOW,
          "Moderate cluster resilience",
          String.format("%.1f%% of simulations had healthy cluster - consider adding capacity",
              healthyProbability * 100)
      ));
    } else {
      recommendations.add(new Recommendation(
          Severity.HIGH,
          "Low cluster resilience",
          String.format("Only %.1f%% of simulations had healthy cluster - add brokers",
              healthyProbability * 100)
      ));
    }

    if (capacityExceededProbability > 0.01) {
      recommendations.add(new Recommendation(
          Severity.MEDIUM,
          "Capacity exceeded risk",
          String.format("%.1f%% probability of exceeding capacity - add headroom",
              capacityExceededProbability * 100)
      ));
    }

    if (dataLossProbability > 0.001) {
      recommendations.add(new Recommendation(
          Severity.CRITICAL,
          "Data loss risk",
          String.format("%.2f%% probability of data loss risk - increase replication factor",
              dataLossProbability * 100)
      ));
    }

    return recommendations;
  }

  /**
   * Configuration for Monte Carlo simulation.
   */
  public static class MonteCarloConfig {
    private final int _iterations;
    private final double _brokerFailureRate;
    private final double _loadVarianceMean;
    private final double _loadVarianceStdDev;

    public MonteCarloConfig(int iterations, double brokerFailureRate,
                            double loadVarianceMean, double loadVarianceStdDev) {
      _iterations = iterations;
      _brokerFailureRate = brokerFailureRate;
      _loadVarianceMean = loadVarianceMean;
      _loadVarianceStdDev = loadVarianceStdDev;
    }

    public int iterations() {
      return _iterations;
    }

    public double brokerFailureRate() {
      return _brokerFailureRate;
    }

    public double loadVarianceMean() {
      return _loadVarianceMean;
    }

    public double loadVarianceStdDev() {
      return _loadVarianceStdDev;
    }
  }

  /**
   * Result of Monte Carlo simulation.
   */
  public static class MonteCarloResult {
    private final int _totalIterations;
    private final int _successfulIterations;
    private final double _clusterHealthyProbability;
    private final double _capacityExceededProbability;
    private final double _dataLossProbability;
    private final List<Recommendation> _recommendations;

    public MonteCarloResult(int totalIterations, int successfulIterations,
                           double clusterHealthyProbability, double capacityExceededProbability,
                           double dataLossProbability, List<Recommendation> recommendations) {
      _totalIterations = totalIterations;
      _successfulIterations = successfulIterations;
      _clusterHealthyProbability = clusterHealthyProbability;
      _capacityExceededProbability = capacityExceededProbability;
      _dataLossProbability = dataLossProbability;
      _recommendations = recommendations;
    }

    public int totalIterations() {
      return _totalIterations;
    }

    public int successfulIterations() {
      return _successfulIterations;
    }

    public double clusterHealthyProbability() {
      return _clusterHealthyProbability;
    }

    public double capacityExceededProbability() {
      return _capacityExceededProbability;
    }

    public double dataLossProbability() {
      return _dataLossProbability;
    }

    public List<Recommendation> recommendations() {
      return _recommendations;
    }
  }
}
