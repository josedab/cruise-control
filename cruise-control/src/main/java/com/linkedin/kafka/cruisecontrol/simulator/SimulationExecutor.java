/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.ClusterStats;
import com.linkedin.kafka.cruisecontrol.simulator.model.ImpactAssessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.Recommendation;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import com.linkedin.kafka.cruisecontrol.simulator.model.SimulationReport;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes simulations and generates analysis reports.
 * <p>
 * This class orchestrates the simulation process by:
 * 1. Building a simulated cluster model using ScenarioBuilder
 * 2. Running optimization on the simulated cluster
 * 3. Analyzing the results using AnalysisEngine
 * 4. Generating a comprehensive simulation report
 * </p>
 */
public class SimulationExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SimulationExecutor.class);

  private final ScenarioBuilder _scenarioBuilder;
  private final GoalOptimizer _goalOptimizer;
  private final AnalysisEngine _analysisEngine;
  private final KafkaCruiseControlConfig _config;

  /**
   * Constructor for SimulationExecutor.
   *
   * @param loadMonitor the load monitor for getting cluster state
   * @param goalOptimizer the goal optimizer for running optimizations
   * @param config the Cruise Control configuration
   */
  public SimulationExecutor(LoadMonitor loadMonitor,
                            GoalOptimizer goalOptimizer,
                            KafkaCruiseControlConfig config) {
    _scenarioBuilder = new ScenarioBuilder(loadMonitor);
    _goalOptimizer = goalOptimizer;
    _analysisEngine = new AnalysisEngine();
    _config = config;
  }

  /**
   * Executes a simulation scenario.
   * <p>
   * Note: This is a simplified implementation. A production version would:
   * - Support custom optimization options
   * - Handle concurrent simulations
   * - Provide progress tracking
   * - Cache results for performance
   * </p>
   *
   * @param scenario the scenario to simulate
   * @param goals the optimization goals to use
   * @param requirements model completeness requirements
   * @return simulation report with results and recommendations
   * @throws Exception if simulation fails
   */
  public SimulationReport execute(Scenario scenario,
                                  List<Goal> goals,
                                  ModelCompletenessRequirements requirements) throws Exception {
    LOG.info("Executing simulation for scenario: {}", scenario.name());
    long startTime = System.currentTimeMillis();

    try {
      // Step 1: Build simulated cluster model
      LOG.debug("Building simulated cluster model");
      ClusterModel simulatedModel = _scenarioBuilder.buildScenario(scenario, requirements);

      // Step 2: Collect cluster statistics
      ClusterStats clusterStats = collectClusterStats(simulatedModel);
      LOG.debug("Cluster stats: {}", clusterStats);

      // Step 3: Run optimization
      LOG.debug("Running optimization with {} goals", goals.size());
      OptimizerResult optimizationResult = runOptimization(simulatedModel, goals);
      LOG.debug("Optimization complete: {} proposals generated", optimizationResult.goalProposals().size());

      // Step 4: Analyze capacity
      LOG.debug("Analyzing capacity");
      CapacityAnalysis capacityAnalysis = _analysisEngine.analyzeCapacity(simulatedModel);

      // Step 5: Assess impact
      LOG.debug("Assessing impact");
      ImpactAssessment impactAssessment = _analysisEngine.assessImpact(simulatedModel, optimizationResult);

      // Step 6: Generate recommendations
      LOG.debug("Generating recommendations");
      List<Recommendation> recommendations = _analysisEngine.generateRecommendations(
          simulatedModel,
          capacityAnalysis,
          impactAssessment
      );

      // Step 7: Create report
      SimulationReport report = new SimulationReport(
          scenario.name(),
          clusterStats,
          optimizationResult,
          capacityAnalysis,
          impactAssessment,
          recommendations
      );

      long duration = System.currentTimeMillis() - startTime;
      LOG.info("Simulation completed in {} ms: {} recommendations generated",
          duration, recommendations.size());

      return report;

    } catch (Exception e) {
      LOG.error("Simulation failed for scenario: {}", scenario.name(), e);
      throw new Exception("Simulation failed: " + e.getMessage(), e);
    }
  }

  /**
   * Collects basic statistics about the cluster.
   *
   * @param model the cluster model
   * @return cluster statistics
   */
  private ClusterStats collectClusterStats(ClusterModel model) {
    int brokerCount = model.brokers().size();
    int partitionCount = model.partitions().size();

    // Count total replicas and data
    int replicaCount = 0;
    long totalDataBytes = 0;

    for (Partition partition : model.partitions()) {
      for (Replica replica : partition.replicas()) {
        replicaCount++;
        // Simplified: use replica size if available
        // In practice, we'd sum up actual disk usage
        totalDataBytes += replica.disk().diskUsage();
      }
    }

    return new ClusterStats(brokerCount, partitionCount, replicaCount, totalDataBytes);
  }

  /**
   * Runs optimization on the cluster model.
   * <p>
   * Note: This is a simplified implementation that uses the goal optimizer's
   * default optimization logic. A production implementation would provide
   * more control over optimization options.
   * </p>
   *
   * @param model the cluster model to optimize
   * @param goals the goals to use for optimization
   * @return optimization result
   * @throws Exception if optimization fails
   */
  private OptimizerResult runOptimization(ClusterModel model, List<Goal> goals) throws Exception {
    // Note: This is simplified. In a real implementation, we would:
    // 1. Create a proper operation progress tracker
    // 2. Set up optimization options (excluded topics, brokers, etc.)
    // 3. Handle different goal configurations
    // 4. Support async execution

    LOG.warn("Optimization integration is simplified in this implementation. " +
        "Full goal-based optimization would require deeper integration with GoalOptimizer.");

    // For now, return a mock result
    // In production, we would call something like:
    // return _goalOptimizer.optimizations(model, goals, operationProgress, ...);

    // Placeholder: Create a basic optimizer result
    // The actual implementation would use the real goal optimizer
    return new OptimizerResult(model.brokerStatsForReporting(_config), null);
  }
}
