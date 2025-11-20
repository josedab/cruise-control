/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ClusterConfig.
 */
public class ClusterConfigTest {

  @Test
  public void testBasicClusterConfig() {
    ClusterConfig config = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092,kafka2:9092")
        .zookeeperConnect("zk1:2181,zk2:2181")
        .displayName("Test Cluster")
        .region("us-east-1")
        .environment("production")
        .enabled(true)
        .build();

    assertEquals("test-cluster", config.clusterId());
    assertEquals("Test Cluster", config.displayName());
    assertEquals("kafka1:9092,kafka2:9092", config.bootstrapServers());
    assertEquals("zk1:2181,zk2:2181", config.zookeeperConnect());
    assertEquals("us-east-1", config.region());
    assertEquals("production", config.environment());
    assertTrue(config.enabled());
  }

  @Test
  public void testDefaultDisplayName() {
    ClusterConfig config = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .build();

    // Display name should default to cluster ID
    assertEquals("test-cluster", config.displayName());
  }

  @Test
  public void testConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("num.concurrent.partition.movements.per.broker", 5);
    overrides.put("max.num.cluster.movements", 50);

    ClusterConfig config = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .configOverrides(overrides)
        .build();

    Map<String, Object> retrievedOverrides = config.configOverrides();
    assertEquals(5, retrievedOverrides.get("num.concurrent.partition.movements.per.broker"));
    assertEquals(50, retrievedOverrides.get("max.num.cluster.movements"));
  }

  @Test
  public void testAddConfigOverride() {
    ClusterConfig config = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .addConfigOverride("key1", "value1")
        .addConfigOverride("key2", "value2")
        .build();

    Map<String, Object> overrides = config.configOverrides();
    assertEquals("value1", overrides.get("key1"));
    assertEquals("value2", overrides.get("key2"));
  }

  @Test(expected = NullPointerException.class)
  public void testNullClusterId() {
    ClusterConfig.builder(null)
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testNullBootstrapServers() {
    ClusterConfig.builder("test-cluster")
        .bootstrapServers(null)
        .zookeeperConnect("zk1:2181")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testNullZookeeperConnect() {
    ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect(null)
        .build();
  }

  @Test
  public void testEquals() {
    ClusterConfig config1 = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .build();

    ClusterConfig config2 = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka2:9092")  // Different servers
        .zookeeperConnect("zk2:2181")
        .build();

    // Equality based on cluster ID only
    assertEquals(config1, config2);
  }

  @Test
  public void testHashCode() {
    ClusterConfig config1 = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .build();

    ClusterConfig config2 = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka2:9092")
        .zookeeperConnect("zk2:2181")
        .build();

    // Same hash code for same cluster ID
    assertEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  public void testToString() {
    ClusterConfig config = ClusterConfig.builder("test-cluster")
        .bootstrapServers("kafka1:9092")
        .zookeeperConnect("zk1:2181")
        .displayName("Test Cluster")
        .region("us-east-1")
        .environment("production")
        .build();

    String str = config.toString();
    assertTrue(str.contains("test-cluster"));
    assertTrue(str.contains("Test Cluster"));
    assertTrue(str.contains("us-east-1"));
    assertTrue(str.contains("production"));
  }
}
