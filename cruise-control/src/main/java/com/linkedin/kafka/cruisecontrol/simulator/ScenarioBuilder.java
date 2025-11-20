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
import org.apache.kafka.common.TopicPartition;
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

  /**
   * Constructor for ScenarioBuilder.
   *
   * @param loadMonitor the load monitor to get base cluster models from
   */
  public ScenarioBuilder(LoadMonitor loadMonitor) {
    _loadMonitor = loadMonitor;
  }

  /**
   * Creates a simulated cluster model from a scenario definition.
   * <p>
   * Note: This is a simplified implementation that works with the current cluster model.
   * In a full implementation, we would create a deep copy of the cluster model first.
   * </p>
   *
   * @param scenario the scenario modifications to apply
   * @param requirements model completeness requirements
   * @return simulated cluster model
   * @throws Exception if cluster model cannot be created
   */
  public ClusterModel buildScenario(Scenario scenario, ModelCompletenessRequirements requirements) throws Exception {
    LOG.info("Building scenario: {}", scenario.name());

    // Get current cluster model from load monitor
    // Note: In production, we would create a deep copy here to avoid modifying the actual cluster
    ClusterModel clusterModel = _loadMonitor.clusterModel(System.currentTimeMillis(),
        requirements,
        null);

    // Apply modifications
    applyModifications(clusterModel, scenario.modifications());

    // Apply failures
    applyFailures(clusterModel, scenario);

    // Apply load multipliers
    applyLoadMultipliers(clusterModel, scenario.modifications());

    LOG.info("Scenario '{}' built successfully: {} brokers, {} partitions",
        scenario.name(), clusterModel.brokers().size(), clusterModel.partitions().size());

    return clusterModel;
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

      // Create rack if it doesn't exist
      Rack rack = model.rack(brokerSpec.rack());
      if (rack == null) {
        model.createRack(brokerSpec.rack());
      }

      // Create broker
      // Note: Using localhost as host since this is a simulation
      model.createBroker(brokerSpec.rack(),
          "simulated-host-" + brokerSpec.id(),
          brokerSpec.id(),
          brokerSpec.capacity(),
          false);
    }

    // Remove brokers (mark as dead)
    for (Integer brokerId : modifications.removedBrokers()) {
      LOG.debug("Removing broker {}", brokerId);
      Broker broker = model.broker(brokerId);
      if (broker != null) {
        model.setBrokerState(brokerId, Broker.State.DEAD);
      } else {
        LOG.warn("Cannot remove broker {} - does not exist", brokerId);
      }
    }

    // Note: Adding partitions dynamically is complex and would require
    // creating replicas and assigning them to brokers. For now, we log this
    // as a limitation.
    if (!modifications.additionalPartitions().isEmpty()) {
      LOG.warn("Adding partitions dynamically is not yet fully supported in this implementation. " +
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
          if (failure.brokerId() != null) {
            Broker broker = model.broker(failure.brokerId());
            if (broker != null) {
              model.setBrokerState(failure.brokerId(), Broker.State.DEAD);
              LOG.debug("Marked broker {} as DEAD", failure.brokerId());
            } else {
              LOG.warn("Cannot mark broker {} as DEAD - does not exist", failure.brokerId());
            }
          }
          break;

        case DISK_FULL:
          if (failure.brokerId() != null) {
            // Note: Setting disk to 100% full is simplified here
            // In a full implementation, we would modify the broker's disk utilization
            LOG.warn("DISK_FULL failure simulation not fully implemented for broker {}", failure.brokerId());
          }
          break;

        case SLOW_BROKER:
          // Note: Simulating latency would require modifying broker metrics
          // This is a placeholder for future implementation
          LOG.warn("SLOW_BROKER failure simulation not fully implemented for broker {}", failure.brokerId());
          break;

        case RACK_FAILURE:
          if (failure.rack() != null) {
            Rack rack = model.rack(failure.rack());
            if (rack != null) {
              // Mark all brokers in the rack as dead
              for (Broker broker : rack.brokers()) {
                model.setBrokerState(broker.id(), Broker.State.DEAD);
                LOG.debug("Marked broker {} in rack {} as DEAD", broker.id(), failure.rack());
              }
            } else {
              LOG.warn("Cannot fail rack {} - does not exist", failure.rack());
            }
          }
          break;

        default:
          LOG.warn("Unknown failure type: {}", failure.type());
      }
    }
  }

  /**
   * Applies load multipliers to a cluster model.
   * <p>
   * Note: This is a simplified implementation. Properly multiplying load would require
   * modifying the replica loads which is complex.
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

    LOG.warn("Load multipliers are specified but not fully implemented in this version. " +
        "This would require modifying replica loads which is complex. Multipliers requested: {}", multipliers);

    // Future implementation would:
    // 1. Iterate through all replicas
    // 2. For each resource type in multipliers, multiply the replica's load for that resource
    // 3. Update the aggregated loads at broker, rack, and cluster levels
  }
}
