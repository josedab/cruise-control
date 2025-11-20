/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link SyntaxValidator}.
 */
public class SyntaxValidatorTest {

  @Test
  public void testValidGoalClasses() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SyntaxValidator validator = new SyntaxValidator();
    ValidationResult result = validator.validate(config);

    assertFalse("Should not have errors for valid goal classes", result.hasErrors());
  }

  @Test
  public void testInvalidGoalClass() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
        "InvalidGoalClassName");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SyntaxValidator validator = new SyntaxValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for invalid goal class", result.hasErrors());
    assertTrue("Error should mention the invalid goal",
        result.getErrors().stream().anyMatch(e -> e.getMessage().contains("InvalidGoalClassName")));
  }

  @Test
  public void testValidBootstrapServersFormat() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9093");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SyntaxValidator validator = new SyntaxValidator();
    ValidationResult result = validator.validate(config);

    assertFalse("Should not have errors for valid bootstrap.servers format", result.hasErrors());
  }

  @Test
  public void testInvalidBootstrapServersFormat() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-format-no-port");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SyntaxValidator validator = new SyntaxValidator();
    ValidationResult result = validator.validate(config);

    assertTrue("Should have errors for invalid bootstrap.servers format", result.hasErrors());
    assertTrue("Error should mention bootstrap.servers",
        result.getErrors().stream().anyMatch(e -> e.getProperty().equals(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG)));
  }

  @Test
  public void testValidZookeeperConnectFormat() {
    Map<String, Object> props = new HashMap<>();
    props.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG, "zk1:2181,zk2:2181,zk3:2181/kafka");
    props.put(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    props.put(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);
    SyntaxValidator validator = new SyntaxValidator();
    ValidationResult result = validator.validate(config);

    assertFalse("Should not have errors for valid zookeeper.connect format with chroot", result.hasErrors());
  }
}
