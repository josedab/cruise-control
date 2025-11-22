/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.simulator.model.BrokerSpec;
import com.linkedin.kafka.cruisecontrol.simulator.model.Failure;
import com.linkedin.kafka.cruisecontrol.simulator.model.FailureType;
import com.linkedin.kafka.cruisecontrol.simulator.model.Modifications;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.BROKER_CAPACITY;
import static org.junit.Assert.*;

/**
 * Unit tests for ScenarioBuilder.
 */
public class ScenarioBuilderTest {

  @Test
  public void testBuildScenarioFromModel() throws SimulationException {
    // Create base cluster model
    ClusterModel baseModel = createTestClusterModel(5);

    // Create scenario with modifications
    Modifications modifications = new Modifications(
        Collections.emptyList(),
        Arrays.asList(0),  // Remove broker 0
        Collections.emptyList(),
        Collections.emptyMap()
    );

    Scenario scenario = new Scenario("Test Scenario", modifications);

    // Build scenario from model (without load monitor)
    ScenarioBuilder builder = new ScenarioBuilder(null, false);
    ClusterModel simulatedModel = builder.buildScenarioFromModel(baseModel, scenario);

    // Verify broker 0 is marked as dead
    Broker broker0 = simulatedModel.broker(0);
    assertNotNull("Broker 0 should exist", broker0);
    assertEquals("Broker 0 should be dead", Broker.State.DEAD, broker0.state());
  }

  @Test
  public void testApplyBrokerDeadFailure() throws SimulationException {
    ClusterModel baseModel = createTestClusterModel(3);

    List<Failure> failures = Arrays.asList(
        new Failure(FailureType.BROKER_DEAD, 1)
    );

    Scenario scenario = new Scenario("Broker Failure Test", new Modifications(), failures);

    ScenarioBuilder builder = new ScenarioBuilder(null, false);
    ClusterModel simulatedModel = builder.buildScenarioFromModel(baseModel, scenario);

    // Verify broker 1 is dead
    assertEquals("Broker 1 should be dead", Broker.State.DEAD, simulatedModel.broker(1).state());
    assertEquals("Broker 0 should be alive", Broker.State.ALIVE, simulatedModel.broker(0).state());
    assertEquals("Broker 2 should be alive", Broker.State.ALIVE, simulatedModel.broker(2).state());
  }

  @Test
  public void testApplyRackFailure() throws SimulationException {
    ClusterModel baseModel = createTestClusterModel(4);

    List<Failure> failures = Arrays.asList(
        new Failure(FailureType.RACK_FAILURE, "rack-0")
    );

    Scenario scenario = new Scenario("Rack Failure Test", new Modifications(), failures);

    ScenarioBuilder builder = new ScenarioBuilder(null, false);
    ClusterModel simulatedModel = builder.buildScenarioFromModel(baseModel, scenario);

    // All brokers in rack-0 should be dead
    for (Broker broker : simulatedModel.brokers()) {
      if (broker.rack().id().equals("rack-0")) {
        assertEquals("Broker " + broker.id() + " in rack-0 should be dead",
            Broker.State.DEAD, broker.state());
      }
    }
  }

  @Test
  public void testApplySlowBrokerFailure() throws SimulationException {
    ClusterModel baseModel = createTestClusterModel(3);

    List<Failure> failures = Arrays.asList(
        new Failure(FailureType.SLOW_BROKER, 2, 500L)
    );

    Scenario scenario = new Scenario("Slow Broker Test", new Modifications(), failures);

    ScenarioBuilder builder = new ScenarioBuilder(null, false);
    ClusterModel simulatedModel = builder.buildScenarioFromModel(baseModel, scenario);

    // Broker 2 should be demoted
    assertEquals("Broker 2 should be demoted", Broker.State.DEMOTED, simulatedModel.broker(2).state());
  }

  @Test
  public void testScenarioWithMultipleFailures() throws SimulationException {
    ClusterModel baseModel = createTestClusterModel(5);

    List<Failure> failures = Arrays.asList(
        new Failure(FailureType.BROKER_DEAD, 0),
        new Failure(FailureType.BROKER_DEAD, 2),
        new Failure(FailureType.SLOW_BROKER, 4, 200L)
    );

    Scenario scenario = new Scenario("Multiple Failures Test", new Modifications(), failures);

    ScenarioBuilder builder = new ScenarioBuilder(null, false);
    ClusterModel simulatedModel = builder.buildScenarioFromModel(baseModel, scenario);

    assertEquals("Broker 0 should be dead", Broker.State.DEAD, simulatedModel.broker(0).state());
    assertEquals("Broker 1 should be alive", Broker.State.ALIVE, simulatedModel.broker(1).state());
    assertEquals("Broker 2 should be dead", Broker.State.DEAD, simulatedModel.broker(2).state());
    assertEquals("Broker 3 should be alive", Broker.State.ALIVE, simulatedModel.broker(3).state());
    assertEquals("Broker 4 should be demoted", Broker.State.DEMOTED, simulatedModel.broker(4).state());
  }

  /**
   * Creates a test cluster model with specified number of brokers.
   */
  private ClusterModel createTestClusterModel(int brokerCount) {
    ModelGeneration generation = new ModelGeneration(0, 0L);
    ClusterModel clusterModel = new ClusterModel(generation, 1.0);

    String rackId = "rack-0";
    clusterModel.createRack(rackId);

    for (int i = 0; i < brokerCount; i++) {
      BrokerCapacityInfo capacity = new BrokerCapacityInfo(BROKER_CAPACITY);
      clusterModel.createBroker(rackId, "host-" + i, i, capacity, false);
    }

    return clusterModel;
  }
}
