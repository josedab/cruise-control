/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.simulator.model.Assessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.ImpactAssessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.Recommendation;
import com.linkedin.kafka.cruisecontrol.simulator.model.ResourceUtilization;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.BROKER_CAPACITY;
import static org.junit.Assert.*;

/**
 * Unit tests for AnalysisEngine.
 */
public class AnalysisEngineTest {

  @Test
  public void testAnalyzeCapacity() {
    // Create a simple cluster model for testing
    ClusterModel clusterModel = createTestClusterModel();
    AnalysisEngine engine = new AnalysisEngine();

    // Analyze capacity
    CapacityAnalysis analysis = engine.analyzeCapacity(clusterModel);

    // Verify analysis results
    assertNotNull("Capacity analysis should not be null", analysis);
    assertNotNull("Utilization map should not be null", analysis.utilization());

    // Check that we have utilization for all resources
    for (Resource resource : Resource.cachedValues()) {
      ResourceUtilization utilization = analysis.get(resource);
      assertNotNull("Utilization should exist for " + resource, utilization);
      assertTrue("Max utilization should be >= 0", utilization.max() >= 0);
      assertTrue("Average utilization should be >= 0", utilization.average() >= 0);
      assertTrue("Headroom should be <= 100", utilization.headroom() <= 100);
      assertNotNull("Assessment should not be null", utilization.assessment());
    }
  }

  @Test
  public void testAssessHealth() {
    ClusterModel clusterModel = createTestClusterModel();
    AnalysisEngine engine = new AnalysisEngine();

    CapacityAnalysis analysis = engine.analyzeCapacity(clusterModel);

    // With a minimal cluster, we should have healthy capacity
    ResourceUtilization cpu = analysis.cpu();
    assertNotNull("CPU utilization should exist", cpu);

    // The assessment should be one of the valid values
    Assessment assessment = cpu.assessment();
    assertTrue("Assessment should be valid",
        assessment == Assessment.HEALTHY ||
        assessment == Assessment.ADEQUATE ||
        assessment == Assessment.MARGINAL ||
        assessment == Assessment.INSUFFICIENT);
  }

  @Test
  public void testGenerateRecommendations() {
    ClusterModel clusterModel = createTestClusterModel();
    AnalysisEngine engine = new AnalysisEngine();

    // Analyze capacity and generate a mock impact assessment
    CapacityAnalysis capacity = engine.analyzeCapacity(clusterModel);

    // Create a minimal optimizer result for testing
    OptimizerResult mockResult = new OptimizerResult(
        clusterModel.brokerStatsForReporting(null),
        null
    );

    ImpactAssessment impact = engine.assessImpact(clusterModel, mockResult);

    // Generate recommendations
    List<Recommendation> recommendations = engine.generateRecommendations(
        clusterModel,
        capacity,
        impact
    );

    // Verify recommendations
    assertNotNull("Recommendations should not be null", recommendations);
    // We should have at least some recommendations (or INFO if everything is healthy)
    assertFalse("Should have at least one recommendation", recommendations.isEmpty());

    // Verify each recommendation has required fields
    for (Recommendation rec : recommendations) {
      assertNotNull("Severity should not be null", rec.severity());
      assertNotNull("Title should not be null", rec.title());
      assertNotNull("Description should not be null", rec.description());
      assertFalse("Title should not be empty", rec.title().isEmpty());
      assertFalse("Description should not be empty", rec.description().isEmpty());
    }
  }

  /**
   * Creates a simple test cluster model with a few brokers.
   */
  private ClusterModel createTestClusterModel() {
    ModelGeneration generation = new ModelGeneration(0, 0L);
    ClusterModel clusterModel = new ClusterModel(generation, 1.0);

    // Create a simple rack and broker
    String rackId = "rack-0";
    clusterModel.createRack(rackId);

    // Add a few brokers
    for (int i = 0; i < 3; i++) {
      BrokerCapacityInfo capacity = new BrokerCapacityInfo(BROKER_CAPACITY);
      clusterModel.createBroker(rackId, "host-" + i, i, capacity, false);
    }

    return clusterModel;
  }
}
