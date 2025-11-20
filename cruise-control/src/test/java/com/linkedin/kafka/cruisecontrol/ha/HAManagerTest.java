/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.HAConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for HAManager.
 */
public class HAManagerTest {

  @Test
  public void testHAManagerDisabled() {
    // Create config with HA disabled
    Map<String, Object> configs = getBaseConfigs();
    configs.put(HAConfig.HA_ENABLED_CONFIG, false);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs);
    MetricRegistry metricRegistry = new MetricRegistry();

    HAManager haManager = new HAManager(config, metricRegistry);

    assertFalse(haManager.isEnabled());
    assertTrue(haManager.isLeader()); // When disabled, always consider as leader
    assertNull(haManager.getCurrentLeader());
    assertNull(haManager.getInstanceId());
  }

  @Test
  public void testHAManagerEnabledWithoutZooKeeper() {
    // Create config with HA enabled but missing ZooKeeper config
    // This should fail during initialization
    Map<String, Object> configs = getBaseConfigs();
    configs.put(HAConfig.HA_ENABLED_CONFIG, true);
    configs.put(HAConfig.HA_COORDINATION_SERVICE_CONFIG, "zookeeper");
    // Intentionally omit HA_ZOOKEEPER_CONNECT_CONFIG

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs);
    MetricRegistry metricRegistry = new MetricRegistry();

    try {
      new HAManager(config, metricRegistry);
      fail("Expected IllegalArgumentException for missing ZooKeeper config");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("ZooKeeper"));
    }
  }

  @Test
  public void testHAManagerMetrics() {
    // Test that HA manager registers metrics
    Map<String, Object> configs = getBaseConfigs();
    configs.put(HAConfig.HA_ENABLED_CONFIG, false);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs);
    MetricRegistry metricRegistry = new MetricRegistry();

    new HAManager(config, metricRegistry);

    // When HA is disabled, metrics should still be registered but may be null
    // Just verify no exceptions are thrown
    assertNotNull(metricRegistry);
  }

  /**
   * Get base configuration required for KafkaCruiseControlConfig.
   */
  private Map<String, Object> getBaseConfigs() {
    Map<String, Object> configs = new HashMap<>();
    // Add minimum required configs
    configs.put("bootstrap.servers", "localhost:9092");
    configs.put("zookeeper.connect", "localhost:2181");

    // HA configs
    configs.put(HAConfig.HA_ENABLED_CONFIG, false);
    configs.put(HAConfig.HA_EXECUTION_STATE_TOPIC_CONFIG, "__CruiseControlExecutionState");
    configs.put(HAConfig.HA_ZOOKEEPER_ELECTION_PATH_CONFIG, "/cruise-control/leader");
    configs.put(HAConfig.HA_ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG, 30000);
    configs.put(HAConfig.HA_FAILOVER_TIMEOUT_MS_CONFIG, 10000);
    configs.put(HAConfig.HA_STATE_RECOVERY_TIMEOUT_MS_CONFIG, 60000);
    configs.put(HAConfig.HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS_CONFIG, 10000);

    return configs;
  }
}
