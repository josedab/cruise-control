/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for simulation model classes.
 */
public class SimulationModelsTest {

  @Test
  public void testScenarioCreation() {
    Modifications modifications = new Modifications(
        Collections.emptyList(),
        Arrays.asList(1, 2, 3),
        Collections.emptyList(),
        Collections.singletonMap("CPU", 1.5)
    );

    List<Failure> failures = Arrays.asList(
        new Failure(FailureType.BROKER_DEAD, 4),
        new Failure(FailureType.RACK_FAILURE, "rack-1")
    );

    Scenario scenario = new Scenario("Test Scenario", modifications, failures);

    assertEquals("Name should match", "Test Scenario", scenario.name());
    assertEquals("Should have 3 removed brokers", 3, scenario.modifications().removedBrokers().size());
    assertEquals("Should have 2 failures", 2, scenario.failures().size());
    assertEquals("Load multiplier should be 1.5",
        Double.valueOf(1.5), scenario.modifications().loadMultipliers().get("CPU"));
  }

  @Test
  public void testFailureTypes() {
    // Broker dead
    Failure brokerDead = new Failure(FailureType.BROKER_DEAD, 5);
    assertEquals(FailureType.BROKER_DEAD, brokerDead.type());
    assertEquals(Integer.valueOf(5), brokerDead.brokerId());
    assertNull(brokerDead.rack());

    // Rack failure
    Failure rackFailure = new Failure(FailureType.RACK_FAILURE, "rack-2");
    assertEquals(FailureType.RACK_FAILURE, rackFailure.type());
    assertEquals("rack-2", rackFailure.rack());
    assertNull(rackFailure.brokerId());

    // Slow broker with latency
    Failure slowBroker = new Failure(FailureType.SLOW_BROKER, 3, 100L);
    assertEquals(FailureType.SLOW_BROKER, slowBroker.type());
    assertEquals(Integer.valueOf(3), slowBroker.brokerId());
    assertEquals(Long.valueOf(100L), slowBroker.latencyMs());
  }

  @Test
  public void testResourceUtilization() {
    ResourceUtilization util = new ResourceUtilization(
        Resource.CPU,
        85.5,
        65.2,
        80.3,
        14.5,
        Assessment.MARGINAL
    );

    assertEquals(Resource.CPU, util.resource());
    assertEquals(85.5, util.max(), 0.01);
    assertEquals(65.2, util.average(), 0.01);
    assertEquals(80.3, util.p95(), 0.01);
    assertEquals(14.5, util.headroom(), 0.01);
    assertEquals(Assessment.MARGINAL, util.assessment());
  }

  @Test
  public void testCapacityAnalysis() {
    Map<Resource, ResourceUtilization> utilization = new HashMap<>();
    utilization.put(Resource.CPU, new ResourceUtilization(
        Resource.CPU, 70.0, 50.0, 65.0, 30.0, Assessment.HEALTHY));
    utilization.put(Resource.DISK, new ResourceUtilization(
        Resource.DISK, 85.0, 70.0, 80.0, 15.0, Assessment.ADEQUATE));

    CapacityAnalysis analysis = new CapacityAnalysis(utilization);

    assertNotNull(analysis.cpu());
    assertNotNull(analysis.disk());
    assertEquals(Assessment.HEALTHY, analysis.cpu().assessment());
    assertEquals(Assessment.ADEQUATE, analysis.disk().assessment());
  }

  @Test
  public void testRecommendation() {
    Recommendation rec = new Recommendation(
        Severity.HIGH,
        "Add brokers",
        "Cluster needs 5 more brokers to handle projected load"
    );

    assertEquals(Severity.HIGH, rec.severity());
    assertEquals("Add brokers", rec.title());
    assertTrue(rec.description().contains("5 more brokers"));
  }

  @Test
  public void testImpactAssessment() {
    List<String> risks = Arrays.asList("Risk 1", "Risk 2");
    ImpactAssessment impact = new ImpactAssessment(
        1000,           // partition movements
        10737418240L,   // 10 GB data to move
        3600000L,       // 1 hour duration
        15.0,           // CPU impact
        25.0,           // network impact
        risks
    );

    assertEquals(1000, impact.partitionMovements());
    assertEquals(10737418240L, impact.dataToMoveBytes());
    assertEquals(3600000L, impact.estimatedDurationMs());
    assertEquals(15.0, impact.cpuImpact(), 0.01);
    assertEquals(25.0, impact.networkImpact(), 0.01);
    assertEquals(2, impact.risks().size());
  }

  @Test
  public void testClusterStats() {
    ClusterStats stats = new ClusterStats(10, 1000, 3000, 5368709120000L);

    assertEquals(10, stats.brokers());
    assertEquals(1000, stats.partitions());
    assertEquals(3000, stats.replicas());
    assertEquals(5368709120000L, stats.totalDataBytes());
  }

  @Test
  public void testAssessmentOrdering() {
    // Verify enum values are in expected order
    Assessment[] values = Assessment.values();
    assertEquals(4, values.length);
    assertEquals(Assessment.HEALTHY, values[0]);
    assertEquals(Assessment.ADEQUATE, values[1]);
    assertEquals(Assessment.MARGINAL, values[2]);
    assertEquals(Assessment.INSUFFICIENT, values[3]);
  }

  @Test
  public void testSeverityOrdering() {
    // Verify severity enum values
    Severity[] values = Severity.values();
    assertEquals(5, values.length);
    assertEquals(Severity.INFO, values[0]);
    assertEquals(Severity.LOW, values[1]);
    assertEquals(Severity.MEDIUM, values[2]);
    assertEquals(Severity.HIGH, values[3]);
    assertEquals(Severity.CRITICAL, values[4]);
  }

  @Test
  public void testModificationsDefaults() {
    Modifications empty = new Modifications();

    assertTrue(empty.additionalBrokers().isEmpty());
    assertTrue(empty.removedBrokers().isEmpty());
    assertTrue(empty.additionalPartitions().isEmpty());
    assertTrue(empty.loadMultipliers().isEmpty());
  }
}
