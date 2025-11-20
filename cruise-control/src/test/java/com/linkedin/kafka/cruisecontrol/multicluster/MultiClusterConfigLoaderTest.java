/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for MultiClusterConfigLoader.
 */
public class MultiClusterConfigLoaderTest {

  @Test
  public void testLoadSingleClusterConfig() {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("zookeeper.connect", "localhost:2181");
    props.setProperty("some.other.config", "value");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(1, registry.size());
    assertTrue(registry.containsCluster("default"));

    ClusterConfig config = registry.getCluster("default");
    assertEquals("default", config.clusterId());
    assertEquals("localhost:9092", config.bootstrapServers());
    assertEquals("localhost:2181", config.zookeeperConnect());
    assertTrue(config.enabled());
  }

  @Test
  public void testLoadMultiClusterConfig() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "us-east,us-west");

    // US East config
    props.setProperty("cluster.us-east.bootstrap.servers", "kafka1-east:9092");
    props.setProperty("cluster.us-east.zookeeper.connect", "zk1-east:2181");
    props.setProperty("cluster.us-east.display.name", "US East Production");
    props.setProperty("cluster.us-east.region", "us-east-1");
    props.setProperty("cluster.us-east.environment", "production");
    props.setProperty("cluster.us-east.enabled", "true");

    // US West config
    props.setProperty("cluster.us-west.bootstrap.servers", "kafka1-west:9092");
    props.setProperty("cluster.us-west.zookeeper.connect", "zk1-west:2181");
    props.setProperty("cluster.us-west.display.name", "US West Production");
    props.setProperty("cluster.us-west.region", "us-west-2");
    props.setProperty("cluster.us-west.environment", "production");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(2, registry.size());

    // Check US East
    ClusterConfig usEast = registry.getCluster("us-east");
    assertNotNull(usEast);
    assertEquals("us-east", usEast.clusterId());
    assertEquals("US East Production", usEast.displayName());
    assertEquals("kafka1-east:9092", usEast.bootstrapServers());
    assertEquals("zk1-east:2181", usEast.zookeeperConnect());
    assertEquals("us-east-1", usEast.region());
    assertEquals("production", usEast.environment());
    assertTrue(usEast.enabled());

    // Check US West
    ClusterConfig usWest = registry.getCluster("us-west");
    assertNotNull(usWest);
    assertEquals("us-west", usWest.clusterId());
    assertEquals("US West Production", usWest.displayName());
    assertEquals("kafka1-west:9092", usWest.bootstrapServers());
    assertEquals("zk1-west:2181", usWest.zookeeperConnect());
    assertEquals("us-west-2", usWest.region());
    assertEquals("production", usWest.environment());
    assertTrue(usWest.enabled()); // Default true
  }

  @Test
  public void testLoadMultiClusterWithDefaults() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "cluster1,cluster2");

    // Default configs
    props.setProperty("default.goals", "RackAwareGoal,ReplicaCapacityGoal");
    props.setProperty("default.self.healing.enabled", "true");

    // Cluster 1 (inherits defaults)
    props.setProperty("cluster.cluster1.bootstrap.servers", "kafka1:9092");
    props.setProperty("cluster.cluster1.zookeeper.connect", "zk1:2181");

    // Cluster 2 (overrides goals)
    props.setProperty("cluster.cluster2.bootstrap.servers", "kafka2:9092");
    props.setProperty("cluster.cluster2.zookeeper.connect", "zk2:2181");
    props.setProperty("cluster.cluster2.goals", "MinTopicLeadersPerBrokerGoal");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(2, registry.size());

    // Check cluster1 has defaults
    ClusterConfig cluster1 = registry.getCluster("cluster1");
    assertEquals("RackAwareGoal,ReplicaCapacityGoal",
        cluster1.configOverrides().get("goals"));
    assertEquals("true", cluster1.configOverrides().get("self.healing.enabled"));

    // Check cluster2 has override
    ClusterConfig cluster2 = registry.getCluster("cluster2");
    assertEquals("MinTopicLeadersPerBrokerGoal",
        cluster2.configOverrides().get("goals"));
    assertEquals("true", cluster2.configOverrides().get("self.healing.enabled"));
  }

  @Test
  public void testLoadMultiClusterDisabledCluster() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "enabled-cluster,disabled-cluster");

    // Enabled cluster
    props.setProperty("cluster.enabled-cluster.bootstrap.servers", "kafka1:9092");
    props.setProperty("cluster.enabled-cluster.zookeeper.connect", "zk1:2181");
    props.setProperty("cluster.enabled-cluster.enabled", "true");

    // Disabled cluster
    props.setProperty("cluster.disabled-cluster.bootstrap.servers", "kafka2:9092");
    props.setProperty("cluster.disabled-cluster.zookeeper.connect", "zk2:2181");
    props.setProperty("cluster.disabled-cluster.enabled", "false");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(2, registry.size());

    ClusterConfig enabled = registry.getCluster("enabled-cluster");
    assertTrue(enabled.enabled());

    ClusterConfig disabled = registry.getCluster("disabled-cluster");
    assertFalse(disabled.enabled());

    // Check enabled clusters filter
    assertEquals(1, registry.getEnabledClusters().size());
  }

  @Test
  public void testLoadWithWhitespaceInClusterList() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", " cluster1 , cluster2 , cluster3 ");

    props.setProperty("cluster.cluster1.bootstrap.servers", "kafka1:9092");
    props.setProperty("cluster.cluster1.zookeeper.connect", "zk1:2181");
    props.setProperty("cluster.cluster2.bootstrap.servers", "kafka2:9092");
    props.setProperty("cluster.cluster2.zookeeper.connect", "zk2:2181");
    props.setProperty("cluster.cluster3.bootstrap.servers", "kafka3:9092");
    props.setProperty("cluster.cluster3.zookeeper.connect", "zk3:2181");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(3, registry.size());
    assertTrue(registry.containsCluster("cluster1"));
    assertTrue(registry.containsCluster("cluster2"));
    assertTrue(registry.containsCluster("cluster3"));
  }

  @Test
  public void testLoadEmptyClustersListFallsBackToSingleCluster() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "");
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("zookeeper.connect", "localhost:2181");

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    assertEquals(1, registry.size());
    assertTrue(registry.containsCluster("default"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadMissingBootstrapServers() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "cluster1");

    // Missing bootstrap.servers
    props.setProperty("cluster.cluster1.zookeeper.connect", "zk1:2181");

    MultiClusterConfigLoader.loadClusters(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadMissingZookeeperConnect() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "cluster1");

    // Missing zookeeper.connect
    props.setProperty("cluster.cluster1.bootstrap.servers", "kafka1:9092");

    MultiClusterConfigLoader.loadClusters(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadSingleClusterMissingBootstrapServers() {
    Properties props = new Properties();
    // Missing bootstrap.servers
    props.setProperty("zookeeper.connect", "localhost:2181");

    MultiClusterConfigLoader.loadClusters(props);
  }

  @Test
  public void testDefaultDisplayName() {
    Properties props = new Properties();
    props.setProperty("multi.cluster.mode.enabled", "true");
    props.setProperty("clusters", "my-cluster");

    props.setProperty("cluster.my-cluster.bootstrap.servers", "kafka:9092");
    props.setProperty("cluster.my-cluster.zookeeper.connect", "zk:2181");
    // No display.name specified

    ClusterRegistry registry = MultiClusterConfigLoader.loadClusters(props);

    ClusterConfig config = registry.getCluster("my-cluster");
    // Should default to cluster ID
    assertEquals("my-cluster", config.displayName());
  }
}
