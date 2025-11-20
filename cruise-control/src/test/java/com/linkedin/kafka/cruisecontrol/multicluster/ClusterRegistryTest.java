/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import java.util.Collection;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ClusterRegistry.
 */
public class ClusterRegistryTest {

  private ClusterRegistry _registry;
  private ClusterConfig _usEastConfig;
  private ClusterConfig _usWestConfig;
  private ClusterConfig _euWestConfig;

  @Before
  public void setUp() {
    _registry = new ClusterRegistry();

    _usEastConfig = ClusterConfig.builder("us-east")
        .bootstrapServers("kafka1-us-east:9092")
        .zookeeperConnect("zk1-us-east:2181")
        .displayName("US East Production")
        .region("us-east-1")
        .environment("production")
        .enabled(true)
        .build();

    _usWestConfig = ClusterConfig.builder("us-west")
        .bootstrapServers("kafka1-us-west:9092")
        .zookeeperConnect("zk1-us-west:2181")
        .displayName("US West Production")
        .region("us-west-2")
        .environment("production")
        .enabled(true)
        .build();

    _euWestConfig = ClusterConfig.builder("eu-west")
        .bootstrapServers("kafka1-eu-west:9092")
        .zookeeperConnect("zk1-eu-west:2181")
        .displayName("EU West Staging")
        .region("eu-west-1")
        .environment("staging")
        .enabled(false)
        .build();
  }

  @Test
  public void testRegisterCluster() {
    _registry.registerCluster(_usEastConfig);

    assertEquals(1, _registry.size());
    assertTrue(_registry.containsCluster("us-east"));
    assertEquals(_usEastConfig, _registry.getCluster("us-east"));
  }

  @Test
  public void testRegisterMultipleClusters() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);
    _registry.registerCluster(_euWestConfig);

    assertEquals(3, _registry.size());
    assertTrue(_registry.listClusters().contains("us-east"));
    assertTrue(_registry.listClusters().contains("us-west"));
    assertTrue(_registry.listClusters().contains("eu-west"));
  }

  @Test
  public void testReplaceCluster() {
    _registry.registerCluster(_usEastConfig);

    // Register again with different config
    ClusterConfig newConfig = ClusterConfig.builder("us-east")
        .bootstrapServers("kafka-new:9092")
        .zookeeperConnect("zk-new:2181")
        .build();

    _registry.registerCluster(newConfig);

    // Should be replaced
    assertEquals(1, _registry.size());
    assertEquals(newConfig, _registry.getCluster("us-east"));
  }

  @Test
  public void testUnregisterCluster() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);

    ClusterConfig removed = _registry.unregisterCluster("us-east");

    assertEquals(_usEastConfig, removed);
    assertEquals(1, _registry.size());
    assertFalse(_registry.containsCluster("us-east"));
    assertTrue(_registry.containsCluster("us-west"));
  }

  @Test
  public void testUnregisterNonExistentCluster() {
    ClusterConfig removed = _registry.unregisterCluster("non-existent");
    assertNull(removed);
  }

  @Test
  public void testGetCluster() {
    _registry.registerCluster(_usEastConfig);

    ClusterConfig retrieved = _registry.getCluster("us-east");
    assertEquals(_usEastConfig, retrieved);

    ClusterConfig nonExistent = _registry.getCluster("non-existent");
    assertNull(nonExistent);
  }

  @Test
  public void testContainsCluster() {
    _registry.registerCluster(_usEastConfig);

    assertTrue(_registry.containsCluster("us-east"));
    assertFalse(_registry.containsCluster("non-existent"));
  }

  @Test
  public void testListClusters() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);

    assertEquals(2, _registry.listClusters().size());
    assertTrue(_registry.listClusters().contains("us-east"));
    assertTrue(_registry.listClusters().contains("us-west"));
  }

  @Test
  public void testGetAllClusters() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);

    Collection<ClusterConfig> allClusters = _registry.getAllClusters();
    assertEquals(2, allClusters.size());
    assertTrue(allClusters.contains(_usEastConfig));
    assertTrue(allClusters.contains(_usWestConfig));
  }

  @Test
  public void testGetClustersByEnvironment() {
    _registry.registerCluster(_usEastConfig);  // production
    _registry.registerCluster(_usWestConfig);  // production
    _registry.registerCluster(_euWestConfig);  // staging

    List<ClusterConfig> prodClusters = _registry.getClustersByEnvironment("production");
    assertEquals(2, prodClusters.size());
    assertTrue(prodClusters.contains(_usEastConfig));
    assertTrue(prodClusters.contains(_usWestConfig));

    List<ClusterConfig> stagingClusters = _registry.getClustersByEnvironment("staging");
    assertEquals(1, stagingClusters.size());
    assertTrue(stagingClusters.contains(_euWestConfig));

    List<ClusterConfig> devClusters = _registry.getClustersByEnvironment("dev");
    assertTrue(devClusters.isEmpty());
  }

  @Test
  public void testGetClustersByEnvironmentNull() {
    _registry.registerCluster(_usEastConfig);

    List<ClusterConfig> clusters = _registry.getClustersByEnvironment(null);
    assertTrue(clusters.isEmpty());
  }

  @Test
  public void testGetClustersByRegion() {
    _registry.registerCluster(_usEastConfig);  // us-east-1
    _registry.registerCluster(_usWestConfig);  // us-west-2
    _registry.registerCluster(_euWestConfig);  // eu-west-1

    List<ClusterConfig> usEastClusters = _registry.getClustersByRegion("us-east-1");
    assertEquals(1, usEastClusters.size());
    assertTrue(usEastClusters.contains(_usEastConfig));

    List<ClusterConfig> euClusters = _registry.getClustersByRegion("eu-west-1");
    assertEquals(1, euClusters.size());
    assertTrue(euClusters.contains(_euWestConfig));

    List<ClusterConfig> apClusters = _registry.getClustersByRegion("ap-south-1");
    assertTrue(apClusters.isEmpty());
  }

  @Test
  public void testGetClustersByRegionNull() {
    _registry.registerCluster(_usEastConfig);

    List<ClusterConfig> clusters = _registry.getClustersByRegion(null);
    assertTrue(clusters.isEmpty());
  }

  @Test
  public void testGetEnabledClusters() {
    _registry.registerCluster(_usEastConfig);   // enabled
    _registry.registerCluster(_usWestConfig);   // enabled
    _registry.registerCluster(_euWestConfig);   // disabled

    List<ClusterConfig> enabledClusters = _registry.getEnabledClusters();
    assertEquals(2, enabledClusters.size());
    assertTrue(enabledClusters.contains(_usEastConfig));
    assertTrue(enabledClusters.contains(_usWestConfig));
    assertFalse(enabledClusters.contains(_euWestConfig));
  }

  @Test
  public void testSize() {
    assertEquals(0, _registry.size());

    _registry.registerCluster(_usEastConfig);
    assertEquals(1, _registry.size());

    _registry.registerCluster(_usWestConfig);
    assertEquals(2, _registry.size());

    _registry.unregisterCluster("us-east");
    assertEquals(1, _registry.size());
  }

  @Test
  public void testIsEmpty() {
    assertTrue(_registry.isEmpty());

    _registry.registerCluster(_usEastConfig);
    assertFalse(_registry.isEmpty());

    _registry.unregisterCluster("us-east");
    assertTrue(_registry.isEmpty());
  }

  @Test
  public void testClear() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);

    assertEquals(2, _registry.size());

    _registry.clear();

    assertTrue(_registry.isEmpty());
    assertEquals(0, _registry.size());
  }

  @Test(expected = NullPointerException.class)
  public void testRegisterNullCluster() {
    _registry.registerCluster(null);
  }

  @Test
  public void testToString() {
    _registry.registerCluster(_usEastConfig);
    _registry.registerCluster(_usWestConfig);

    String str = _registry.toString();
    assertTrue(str.contains("clusterCount=2"));
    assertTrue(str.contains("us-east"));
    assertTrue(str.contains("us-west"));
  }
}
