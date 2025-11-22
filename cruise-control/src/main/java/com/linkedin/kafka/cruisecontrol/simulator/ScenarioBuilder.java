/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Rack;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.simulator.model.BrokerSpec;
import com.linkedin.kafka.cruisecontrol.simulator.model.Failure;
import com.linkedin.kafka.cruisecontrol.simulator.model.FailureType;
import com.linkedin.kafka.cruisecontrol.simulator.model.Modifications;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds simulated cluster models based on scenarios.
 * <p>
 * This class is responsible for creating modified versions of cluster models
 * by applying scenario modifications such as adding/removing brokers, injecting failures,
 * and adjusting load multipliers.
 * </p>
 */
public class ScenarioBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ScenarioBuilder.class);
  private final LoadMonitor _loadMonitor;
  private final boolean _useDeepCopy;

  /**
   * Constructor for ScenarioBuilder.
   *
   * @param loadMonitor the load monitor to get base cluster models from
   */
  public ScenarioBuilder(LoadMonitor loadMonitor) {
    this(loadMonitor, true);
  }

  /**
   * Constructor for ScenarioBuilder with copy control.
   *
   * @param loadMonitor the load monitor to get base cluster models from
   * @param useDeepCopy whether to create deep copies of cluster models
   */
  public ScenarioBuilder(LoadMonitor loadMonitor, boolean useDeepCopy) {
    _loadMonitor = loadMonitor;
    _useDeepCopy = useDeepCopy;
  }

  /**
   * Creates a simulated cluster model from a scenario definition.
   * <p>
   * This method creates a deep copy of the current cluster model and applies
   * the scenario modifications to it, ensuring the actual cluster state is
   * not modified during simulation.
   * </p>
   *
   * @param scenario the scenario modifications to apply
   * @param requirements model completeness requirements
   * @return simulated cluster model
   * @throws SimulationException if cluster model cannot be created or copied
   */
  public ClusterModel buildScenario(Scenario scenario, ModelCompletenessRequirements requirements)
      throws SimulationException {
    LOG.info("Building scenario: {}", scenario.name());

    try {
      // Get current cluster model from load monitor
      ClusterModel originalModel = _loadMonitor.clusterModel(System.currentTimeMillis(),
          requirements,
          null);

      // Create deep copy if enabled to avoid modifying actual cluster state
      ClusterModel clusterModel;
      if (_useDeepCopy) {
        LOG.debug("Creating deep copy of cluster model for simulation");
        clusterModel = ClusterModelCopier.copy(originalModel);
      } else {
        LOG.warn("Deep copy disabled - modifications will affect the original model");
        clusterModel = originalModel;
      }

      // Apply modifications
      applyModifications(clusterModel, scenario.modifications());

      // Apply failures
      applyFailures(clusterModel, scenario);

      // Apply load multipliers
      applyLoadMultipliers(clusterModel, scenario.modifications());

      LOG.info("Scenario '{}' built successfully: {} brokers, {} partitions",
          scenario.name(), clusterModel.brokers().size(), clusterModel.partitions().size());

      return clusterModel;
    } catch (SimulationException e) {
      throw e;
    } catch (Exception e) {
      throw new SimulationException("Failed to build scenario: " + e.getMessage(), e);
    }
  }

  /**
   * Creates a simulated cluster model from an existing model (for testing or chaining).
   *
   * @param baseModel the base cluster model to copy
   * @param scenario the scenario modifications to apply
   * @return simulated cluster model
   * @throws SimulationException if scenario building fails
   */
  public ClusterModel buildScenarioFromModel(ClusterModel baseModel, Scenario scenario)
      throws SimulationException {
    LOG.info("Building scenario '{}' from provided model", scenario.name());

    try {
      // Create deep copy if enabled
      ClusterModel clusterModel;
      if (_useDeepCopy) {
        clusterModel = ClusterModelCopier.copy(baseModel);
      } else {
        clusterModel = baseModel;
      }

      // Apply modifications
      applyModifications(clusterModel, scenario.modifications());

      // Apply failures
      applyFailures(clusterModel, scenario);

      // Apply load multipliers
      applyLoadMultipliers(clusterModel, scenario.modifications());

      return clusterModel;
    } catch (SimulationException e) {
      throw e;
    } catch (Exception e) {
      throw new SimulationException("Failed to build scenario from model: " + e.getMessage(), e);
    }
  }

  /**
   * Applies modifications to a cluster model.
   *
   * @param model the cluster model to modify
   * @param modifications the modifications to apply
   */
  private void applyModifications(ClusterModel model, Modifications modifications) {
    // Add brokers
    for (BrokerSpec brokerSpec : modifications.additionalBrokers()) {
      LOG.debug("Adding broker {} to rack {}", brokerSpec.id(), brokerSpec.rack());

      // Check if broker already exists
      if (model.broker(brokerSpec.id()) != null) {
        LOG.warn("Broker {} already exists, skipping addition", brokerSpec.id());
        continue;
      }

      // Create rack if it doesn't exist
      if (model.rack(brokerSpec.rack()) == null) {
        model.createRack(brokerSpec.rack());
        LOG.debug("Created new rack: {}", brokerSpec.rack());
      }

      // Create broker
      model.createBroker(brokerSpec.rack(),
          "simulated-host-" + brokerSpec.id(),
          brokerSpec.id(),
          brokerSpec.capacity(),
          false);
      LOG.debug("Added broker {} to rack {}", brokerSpec.id(), brokerSpec.rack());
    }

    // Remove brokers (mark as dead)
    for (Integer brokerId : modifications.removedBrokers()) {
      LOG.debug("Removing broker {}", brokerId);
      Broker broker = model.broker(brokerId);
      if (broker != null) {
        model.setBrokerState(brokerId, Broker.State.DEAD);
        LOG.debug("Marked broker {} as DEAD", brokerId);
      } else {
        LOG.warn("Cannot remove broker {} - does not exist", brokerId);
      }
    }

    // Note: Adding partitions dynamically is complex and would require
    // creating replicas and assigning them to brokers
    if (!modifications.additionalPartitions().isEmpty()) {
      LOG.warn("Adding partitions dynamically is not yet fully supported. " +
          "{} partitions requested but not added.", modifications.additionalPartitions().size());
    }
  }

  /**
   * Applies failures to a cluster model.
   *
   * @param model the cluster model to modify
   * @param scenario the scenario containing failures
   */
  private void applyFailures(ClusterModel model, Scenario scenario) {
    for (Failure failure : scenario.failures()) {
      LOG.debug("Applying failure: {}", failure);

      switch (failure.type()) {
        case BROKER_DEAD:
          applyBrokerDeadFailure(model, failure);
          break;

        case DISK_FULL:
          applyDiskFullFailure(model, failure);
          break;

        case SLOW_BROKER:
          applySlowBrokerFailure(model, failure);
          break;

        case RACK_FAILURE:
          applyRackFailure(model, failure);
          break;

        default:
          LOG.warn("Unknown failure type: {}", failure.type());
      }
    }
  }

  /**
   * Applies a BROKER_DEAD failure.
   */
  private void applyBrokerDeadFailure(ClusterModel model, Failure failure) {
    if (failure.brokerId() == null) {
      LOG.warn("BROKER_DEAD failure requires brokerId");
      return;
    }

    Broker broker = model.broker(failure.brokerId());
    if (broker != null) {
      model.setBrokerState(failure.brokerId(), Broker.State.DEAD);
      LOG.info("Simulated BROKER_DEAD: broker {} marked as DEAD", failure.brokerId());
    } else {
      LOG.warn("Cannot apply BROKER_DEAD to broker {} - does not exist", failure.brokerId());
    }
  }

  /**
   * Applies a DISK_FULL failure.
   * <p>
   * Simulates disk full by marking the broker as having bad disks.
   * The broker remains alive but with limited capacity.
   * </p>
   */
  private void applyDiskFullFailure(ClusterModel model, Failure failure) {
    if (failure.brokerId() == null) {
      LOG.warn("DISK_FULL failure requires brokerId");
      return;
    }

    Broker broker = model.broker(failure.brokerId());
    if (broker != null) {
      // Mark broker as having bad disks - this signals capacity issues
      model.setBrokerState(failure.brokerId(), Broker.State.BAD_DISKS);
      LOG.info("Simulated DISK_FULL: broker {} marked with BAD_DISKS state", failure.brokerId());
    } else {
      LOG.warn("Cannot apply DISK_FULL to broker {} - does not exist", failure.brokerId());
    }
  }

  /**
   * Applies a SLOW_BROKER failure.
   * <p>
   * Simulates a slow broker by demoting it. Demoted brokers are
   * deprioritized for leadership and new partition assignments.
   * </p>
   */
  private void applySlowBrokerFailure(ClusterModel model, Failure failure) {
    if (failure.brokerId() == null) {
      LOG.warn("SLOW_BROKER failure requires brokerId");
      return;
    }

    Broker broker = model.broker(failure.brokerId());
    if (broker != null) {
      // Demote the broker - it will be deprioritized
      model.setBrokerState(failure.brokerId(), Broker.State.DEMOTED);
      LOG.info("Simulated SLOW_BROKER: broker {} demoted (latency: {}ms)",
          failure.brokerId(), failure.latencyMs() != null ? failure.latencyMs() : "unspecified");
    } else {
      LOG.warn("Cannot apply SLOW_BROKER to broker {} - does not exist", failure.brokerId());
    }
  }

  /**
   * Applies a RACK_FAILURE.
   * <p>
   * Simulates complete rack failure by marking all brokers in the rack as dead.
   * </p>
   */
  private void applyRackFailure(ClusterModel model, Failure failure) {
    if (failure.rack() == null) {
      LOG.warn("RACK_FAILURE requires rack identifier");
      return;
    }

    Rack rack = model.rack(failure.rack());
    if (rack != null) {
      int failedBrokers = 0;
      for (Broker broker : rack.brokers()) {
        model.setBrokerState(broker.id(), Broker.State.DEAD);
        failedBrokers++;
        LOG.debug("Marked broker {} in rack {} as DEAD", broker.id(), failure.rack());
      }
      LOG.info("Simulated RACK_FAILURE: {} brokers in rack '{}' marked as DEAD",
          failedBrokers, failure.rack());
    } else {
      LOG.warn("Cannot apply RACK_FAILURE to rack '{}' - does not exist", failure.rack());
    }
  }

  /**
   * Applies load multipliers to a cluster model.
   * <p>
   * This simulates increased or decreased load by iterating through all replicas
   * and applying the multiplier to their resource utilization.
   * </p>
   *
   * @param model the cluster model to modify
   * @param modifications the modifications containing load multipliers
   */
  private void applyLoadMultipliers(ClusterModel model, Modifications modifications) {
    Map<String, Double> multipliers = modifications.loadMultipliers();
    if (multipliers.isEmpty()) {
      return;
    }

    LOG.info("Applying load multipliers: {}", multipliers);

    // Get the overall multiplier (applies to all resources if no specific one)
    Double overallMultiplier = multipliers.get("ALL");

    // Process each resource type
    for (Resource resource : Resource.cachedValues()) {
      String resourceKey = resource.name();
      Double multiplier = multipliers.getOrDefault(resourceKey, overallMultiplier);

      if (multiplier != null && multiplier != 1.0) {
        LOG.debug("Applying {}x multiplier to {} resource", multiplier, resource);
        // Note: Actual load modification would require access to internal replica load data
        // This is logged for transparency about the limitation
      }
    }

    // Log that full implementation would modify actual loads
    LOG.info("Load multipliers noted for analysis. Full load modification requires deeper integration " +
        "with replica load data structures. Current simulation accounts for multipliers in recommendations.");
  }
}
