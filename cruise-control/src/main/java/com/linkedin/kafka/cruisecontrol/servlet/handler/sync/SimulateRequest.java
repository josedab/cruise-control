/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.SimulationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.SimulationResult;
import com.linkedin.kafka.cruisecontrol.simulator.SimulationException;
import com.linkedin.kafka.cruisecontrol.simulator.SimulationExecutor;
import com.linkedin.kafka.cruisecontrol.simulator.model.Failure;
import com.linkedin.kafka.cruisecontrol.simulator.model.FailureType;
import com.linkedin.kafka.cruisecontrol.simulator.model.Modifications;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import com.linkedin.kafka.cruisecontrol.simulator.model.SimulationReport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SIMULATION_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * Handler for simulation requests.
 * <p>
 * This handler processes what-if analysis requests and returns simulation results
 * including capacity analysis, impact assessment, and recommendations.
 * </p>
 */
public class SimulateRequest extends AbstractSyncRequest {
  private static final Logger LOG = LoggerFactory.getLogger(SimulateRequest.class);

  private SimulationParameters _parameters;
  private SimulationExecutor _simulationExecutor;
  private KafkaCruiseControlConfig _config;
  private GoalOptimizer _goalOptimizer;

  public SimulateRequest() {
    super();
  }

  @Override
  protected CruiseControlResponse handle() {
    try {
      LOG.info("Processing simulation request: {}", _parameters.scenarioName());

      // Build scenario from parameters
      Scenario scenario = buildScenarioFromParameters();

      // Get goals from parameters or use defaults
      List<Goal> goals = getGoals();

      // Get model completeness requirements
      ModelCompletenessRequirements requirements = _goalOptimizer.defaultModelCompletenessRequirements();

      // Execute simulation
      SimulationReport report = _simulationExecutor.execute(scenario, goals, requirements);

      return new SimulationResult(report, _config);

    } catch (SimulationException e) {
      LOG.error("Simulation failed", e);
      throw new RuntimeException("Simulation failed: " + e.getMessage(), e);
    }
  }

  /**
   * Builds a scenario from the request parameters.
   */
  private Scenario buildScenarioFromParameters() {
    List<Failure> failures = new ArrayList<>();

    // Parse fail_brokers parameter
    String failBrokers = _parameters.failBrokers();
    if (failBrokers != null && !failBrokers.isEmpty()) {
      for (String brokerId : failBrokers.split(",")) {
        try {
          failures.add(new Failure(FailureType.BROKER_DEAD, Integer.parseInt(brokerId.trim())));
        } catch (NumberFormatException e) {
          LOG.warn("Invalid broker ID in fail_brokers: {}", brokerId);
        }
      }
    }

    // Parse fail_rack parameter
    String failRack = _parameters.failRack();
    if (failRack != null && !failRack.isEmpty()) {
      failures.add(new Failure(FailureType.RACK_FAILURE, failRack));
    }

    // Build modifications (add/remove brokers)
    Modifications modifications = new Modifications(
        Collections.emptyList(),  // Additional brokers - would need more params
        parseRemovedBrokers(),
        Collections.emptyList(),  // Additional partitions
        Collections.emptyMap()    // Load multipliers
    );

    return new Scenario(_parameters.scenarioName(), modifications, failures);
  }

  /**
   * Parses removed brokers from parameters.
   */
  private List<Integer> parseRemovedBrokers() {
    String removeBrokers = _parameters.removeBrokers();
    if (removeBrokers == null || removeBrokers.isEmpty()) {
      return Collections.emptyList();
    }

    List<Integer> brokerIds = new ArrayList<>();
    for (String brokerId : removeBrokers.split(",")) {
      try {
        brokerIds.add(Integer.parseInt(brokerId.trim()));
      } catch (NumberFormatException e) {
        LOG.warn("Invalid broker ID in remove_brokers: {}", brokerId);
      }
    }
    return brokerIds;
  }

  /**
   * Gets goals for simulation.
   */
  private List<Goal> getGoals() {
    // Use goals from parameters if specified, otherwise use defaults
    List<Goal> goals = _parameters.goals();
    if (goals != null && !goals.isEmpty()) {
      return goals;
    }

    // Fall back to default goals from goal optimizer
    return _goalOptimizer.defaultGoals();
  }

  @Override
  public CruiseControlParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return SimulateRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (SimulationParameters) validateNotNull(configs.get(SIMULATION_PARAMETER_OBJECT_CONFIG),
        "Parameter configuration is missing from the request.");

    KafkaCruiseControlEndPoints endpoints = getCruiseControlEndpoints();
    _config = endpoints.config();
    _goalOptimizer = endpoints.goalOptimizer();
    LoadMonitor loadMonitor = endpoints.loadMonitor();

    _simulationExecutor = new SimulationExecutor(loadMonitor, _goalOptimizer, _config);
  }
}
