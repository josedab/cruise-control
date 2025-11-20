/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link RangeValidator}.
 */
public class RangeValidatorTest {

  private Map<String, Object> getBaseConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    return props;
  }

  @Test
  public void testValidBalanceThreshold() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, 1.10);
    props.put(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, 1.05);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertFalse("Should not have errors for valid balance thresholds", result.hasErrors());
  }

  @Test
  public void testInvalidBalanceThresholdTooLow() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, 0.9);  // Must be >= 1.0

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for balance threshold < 1.0", result.hasErrors());
    assertTrue("Error should mention the threshold property",
        result.getErrors().stream().anyMatch(e ->
            e.getProperty().equals(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG)));
  }

  @Test
  public void testValidPositiveIntegers() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, 50);
    props.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, 10);
    props.put(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG, 100);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertFalse("Should not have errors for valid positive integers", result.hasErrors());
  }

  @Test
  public void testInvalidNegativeInteger() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, -5);  // Must be positive

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for negative integer", result.hasErrors());
    assertTrue("Error should mention the property",
        result.getErrors().stream().anyMatch(e ->
            e.getProperty().equals(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG)));
  }

  @Test
  public void testInvalidPercentage() {
    Map<String, Object> props = getBaseConfig();
    props.put(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, 150.0);  // > 100

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for percentage > 100", result.hasErrors());
    assertTrue("Error should mention the percentage property",
        result.getErrors().stream().anyMatch(e ->
            e.getProperty().equals(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG)));
  }

  @Test
  public void testZeroNotAllowedForPositiveInt() {
    Map<String, Object> props = getBaseConfig();
    props.put(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, 0);  // Must be > 0

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    RangeValidator validator = new RangeValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for zero when positive required", result.hasErrors());
  }
}
