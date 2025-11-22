/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
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
import java.util.ArrayList;
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
  private final LoadMonitor _loadMonitor;

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
    _loadMonitor = loadMonitor;
    _scenarioBuilder = new ScenarioBuilder(loadMonitor);
    _goalOptimizer = goalOptimizer;
    _analysisEngine = new AnalysisEngine();
    _config = config;
  }

  /**
   * Executes a simulation scenario.
   *
   * @param scenario the scenario to simulate
   * @param goals the optimization goals to use
   * @param requirements model completeness requirements
   * @return simulation report with results and recommendations
   * @throws SimulationException if simulation fails
   */
  public SimulationReport execute(Scenario scenario,
                                  List<Goal> goals,
                                  ModelCompletenessRequirements requirements) throws SimulationException {
    return execute(scenario, goals, requirements, new OperationProgress());
  }

  /**
   * Executes a simulation scenario with progress tracking.
   *
   * @param scenario the scenario to simulate
   * @param goals the optimization goals to use
   * @param requirements model completeness requirements
   * @param operationProgress progress tracker
   * @return simulation report with results and recommendations
   * @throws SimulationException if simulation fails
   */
  public SimulationReport execute(Scenario scenario,
                                  List<Goal> goals,
                                  ModelCompletenessRequirements requirements,
                                  OperationProgress operationProgress) throws SimulationException {
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
      OptimizerResult optimizationResult = runOptimization(simulatedModel, goals, operationProgress);
      LOG.debug("Optimization complete: {} proposals generated",
          optimizationResult != null ? optimizationResult.goalProposals().size() : 0);

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

    } catch (SimulationException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Simulation failed for scenario: {}", scenario.name(), e);
      throw new SimulationException("Simulation failed: " + e.getMessage(), e);
    }
  }

  /**
   * Executes multiple scenarios and returns comparison results.
   *
   * @param scenarios list of scenarios to simulate
   * @param goals the optimization goals to use
   * @param requirements model completeness requirements
   * @return list of simulation reports for comparison
   * @throws SimulationException if any simulation fails
   */
  public List<SimulationReport> executeComparison(List<Scenario> scenarios,
                                                  List<Goal> goals,
                                                  ModelCompletenessRequirements requirements) throws SimulationException {
    LOG.info("Executing comparison simulation for {} scenarios", scenarios.size());

    List<SimulationReport> reports = new ArrayList<>();
    for (Scenario scenario : scenarios) {
      try {
        SimulationReport report = execute(scenario, goals, requirements);
        reports.add(report);
      } catch (SimulationException e) {
        LOG.warn("Scenario '{}' failed: {}", scenario.name(), e.getMessage());
        // Continue with other scenarios
      }
    }

    return reports;
  }

  /**
   * Collects basic statistics about the cluster.
   *
   * @param model the cluster model
   * @return cluster statistics
   */
  private ClusterStats collectClusterStats(ClusterModel model) {
    int brokerCount = model.brokers().size();
    int aliveBrokerCount = model.aliveBrokers().size();
    int deadBrokerCount = model.deadBrokers().size();
    int partitionCount = model.partitions().size();

    // Count total replicas and data
    int replicaCount = 0;
    long totalDataBytes = 0;

    for (Partition partition : model.partitions()) {
      for (Replica replica : partition.replicas()) {
        replicaCount++;
        if (replica.disk() != null) {
          totalDataBytes += replica.disk().diskUsage();
        }
      }
    }

    LOG.debug("Cluster stats: brokers={} (alive={}, dead={}), partitions={}, replicas={}",
        brokerCount, aliveBrokerCount, deadBrokerCount, partitionCount, replicaCount);

    return new ClusterStats(brokerCount, partitionCount, replicaCount, totalDataBytes);
  }

  /**
   * Runs optimization on the cluster model using GoalOptimizer.
   *
   * @param model the cluster model to optimize
   * @param goals the goals to use for optimization
   * @param operationProgress progress tracker
   * @return optimization result
   * @throws SimulationException if optimization fails
   */
  private OptimizerResult runOptimization(ClusterModel model,
                                          List<Goal> goals,
                                          OperationProgress operationProgress) throws SimulationException {
    try {
      // Check if cluster is alive
      if (!model.isClusterAlive()) {
        LOG.warn("All brokers are dead in the simulated cluster - cannot run optimization");
        // Return a result with broker stats but no proposals
        return new OptimizerResult(model.brokerStats(_config), null);
      }

      // Check if we have goals
      if (goals == null || goals.isEmpty()) {
        LOG.warn("No goals specified for optimization");
        return new OptimizerResult(model.brokerStats(_config), null);
      }

      // Run optimization using GoalOptimizer
      LOG.debug("Running optimization with goals: {}",
          goals.stream().map(g -> g.getClass().getSimpleName()).toList());

      OptimizerResult result = _goalOptimizer.optimizations(model, goals, operationProgress);

      LOG.debug("Optimization generated {} proposals",
          result.goalProposals() != null ? result.goalProposals().size() : 0);

      return result;

    } catch (KafkaCruiseControlException e) {
      LOG.warn("Optimization failed: {}", e.getMessage());
      // Return a result with broker stats but no proposals
      return new OptimizerResult(model.brokerStats(_config), null);
    }
  }
}
