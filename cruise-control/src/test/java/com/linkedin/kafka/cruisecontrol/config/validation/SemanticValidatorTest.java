/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link SemanticValidator}.
 */
public class SemanticValidatorTest {

  private Map<String, Object> getBaseConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    return props;
  }

  @Test
  public void testAnomalyDetectionWithSelfHealing() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put("self.healing.enabled", "true");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should not have errors when self-healing is enabled
    assertFalse("Should not have errors when anomaly detection has self-healing enabled",
        result.hasErrors());
  }

  @Test
  public void testAnomalyDetectionWithNotifier() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should not have errors when notifier is configured
    assertFalse("Should not have errors when anomaly detection has notifier configured",
        result.hasErrors());
  }

  @Test
  public void testAnomalyDetectionWithoutAction() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    // No self-healing or notifier configured

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should have a warning when no action is configured
    assertTrue("Should have warnings when anomaly detection has no action",
        result.hasWarnings());
    assertTrue("Warning should mention anomaly detection",
        result.getWarnings().stream().anyMatch(w ->
            w.getProperty().equals(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG)));
  }

  @Test
  public void testThrottlingConsistency() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, 100);
    props.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, 5);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should not have errors for reasonable throttling config
    assertFalse("Should not have errors for reasonable throttling configuration",
        result.hasErrors());
  }

  @Test
  public void testThrottlingInconsistency() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, 1000);
    props.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, 2);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should have a warning for potentially misconfigured throttling
    assertTrue("Should have warnings for potentially misconfigured throttling",
        result.hasWarnings());
  }

  @Test
  public void testClusterMaxLessThanBrokerMax() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, 5);
    props.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, 10);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should have an error when cluster max is less than broker max
    assertTrue("Should have errors when cluster max < broker max", result.hasErrors());
    assertTrue("Error should mention cluster movements",
        result.getErrors().stream().anyMatch(e ->
            e.getProperty().equals(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG)));
  }

  @Test
  public void testGoalOrderingRackAwareFirst() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should not have warnings when RackAwareGoal is first
    assertFalse("Should not have warnings when RackAwareGoal is first",
        result.getWarnings().stream().anyMatch(w ->
            w.getMessage().contains("RackAwareGoal") && w.getMessage().contains("first")));
  }

  @Test
  public void testGoalOrderingRackAwareNotFirst() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SemanticValidator validator = new SemanticValidator();
    ValidationResult result = validator.validate(config);

    // Should have a warning when RackAwareGoal is not first
    assertTrue("Should have warnings when RackAwareGoal is not first",
        result.getWarnings().stream().anyMatch(w ->
            w.getMessage().contains("RackAwareGoal") && w.getMessage().contains("first")));
  }
}
