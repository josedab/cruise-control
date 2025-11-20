/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MultiClusterConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for loading multi-cluster configurations from properties.
 *
 * <p>This class handles parsing of multi-cluster configuration properties and
 * creates ClusterConfig objects for each registered cluster.
 *
 * <p>Configuration format:
 * <pre>
 * # Enable multi-cluster mode
 * multi.cluster.mode.enabled=true
 *
 * # List of cluster IDs
 * clusters=us-east,us-west,eu-west
 *
 * # Cluster-specific configs
 * cluster.us-east.bootstrap.servers=kafka1:9092,kafka2:9092
 * cluster.us-east.zookeeper.connect=zk1:2181,zk2:2181
 * cluster.us-east.display.name=US East Production
 * cluster.us-east.region=us-east-1
 * cluster.us-east.environment=production
 * cluster.us-east.enabled=true
 *
 * # Default configs (apply to all clusters unless overridden)
 * default.goals=RackAwareGoal,ReplicaCapacityGoal,...
 * default.self.healing.enabled=true
 *
 * # Cluster-specific overrides
 * cluster.us-east.goals=RackAwareGoal,MinTopicLeadersPerBrokerGoal,...
 * </pre>
 */
public class MultiClusterConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiClusterConfigLoader.class);

  /**
   * Loads cluster configurations from properties.
   *
   * @param properties The properties containing multi-cluster configuration
   * @return A ClusterRegistry populated with cluster configurations
   * @throws IllegalArgumentException if configuration is invalid
   */
  public static ClusterRegistry loadClusters(Properties properties) {
    ClusterRegistry registry = new ClusterRegistry();

    // Check if multi-cluster mode is enabled
    boolean multiClusterEnabled = Boolean.parseBoolean(
        properties.getProperty(MultiClusterConfig.MULTI_CLUSTER_MODE_ENABLED_CONFIG,
            String.valueOf(MultiClusterConfig.DEFAULT_MULTI_CLUSTER_MODE_ENABLED)));

    if (!multiClusterEnabled) {
      LOG.info("Multi-cluster mode is disabled. Creating default single-cluster configuration.");
      ClusterConfig defaultCluster = loadSingleClusterConfig(properties);
      registry.registerCluster(defaultCluster);
      return registry;
    }

    // Get list of cluster IDs
    String clustersStr = properties.getProperty(MultiClusterConfig.CLUSTERS_CONFIG, "");
    if (clustersStr.trim().isEmpty()) {
      LOG.warn("Multi-cluster mode enabled but no clusters specified. "
          + "Falling back to single-cluster mode.");
      ClusterConfig defaultCluster = loadSingleClusterConfig(properties);
      registry.registerCluster(defaultCluster);
      return registry;
    }

    List<String> clusterIds = Arrays.stream(clustersStr.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    LOG.info("Loading {} clusters: {}", clusterIds.size(), clusterIds);

    // Load default configuration
    Map<String, Object> defaultConfig = loadDefaultConfig(properties);

    // Load each cluster
    for (String clusterId : clusterIds) {
      try {
        ClusterConfig clusterConfig = loadClusterConfig(clusterId, properties, defaultConfig);
        registry.registerCluster(clusterConfig);
        LOG.info("Loaded configuration for cluster '{}'", clusterId);
      } catch (Exception e) {
        LOG.error("Failed to load configuration for cluster '{}'", clusterId, e);
        throw new IllegalArgumentException("Failed to load cluster " + clusterId, e);
      }
    }

    return registry;
  }

  /**
   * Loads configuration for a single cluster (backward compatibility mode).
   *
   * @param properties The properties
   * @return A ClusterConfig for the default cluster
   */
  private static ClusterConfig loadSingleClusterConfig(Properties properties) {
    String bootstrapServers = properties.getProperty("bootstrap.servers");
    String zookeeperConnect = properties.getProperty("zookeeper.connect");

    if (bootstrapServers == null || zookeeperConnect == null) {
      throw new IllegalArgumentException(
          "bootstrap.servers and zookeeper.connect are required in single-cluster mode");
    }

    Map<String, Object> configOverrides = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      configOverrides.put(key, properties.getProperty(key));
    }

    return ClusterConfig.builder("default")
        .bootstrapServers(bootstrapServers)
        .zookeeperConnect(zookeeperConnect)
        .displayName("Default Cluster")
        .enabled(true)
        .configOverrides(configOverrides)
        .build();
  }

  /**
   * Loads default configuration that applies to all clusters.
   *
   * @param properties The properties
   * @return A map of default configuration
   */
  private static Map<String, Object> loadDefaultConfig(Properties properties) {
    Map<String, Object> defaultConfig = new HashMap<>();

    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith("default.")) {
        // Remove "default." prefix
        String configKey = key.substring("default.".length());
        defaultConfig.put(configKey, properties.getProperty(key));
      } else if (!key.startsWith("cluster.") && !key.equals(MultiClusterConfig.CLUSTERS_CONFIG)
          && !key.equals(MultiClusterConfig.MULTI_CLUSTER_MODE_ENABLED_CONFIG)) {
        // Include global configs that don't start with "cluster." or "default."
        defaultConfig.put(key, properties.getProperty(key));
      }
    }

    return defaultConfig;
  }

  /**
   * Loads configuration for a specific cluster.
   *
   * @param clusterId The cluster ID
   * @param properties The properties
   * @param defaultConfig The default configuration to merge with
   * @return A ClusterConfig for the cluster
   */
  private static ClusterConfig loadClusterConfig(String clusterId,
                                                   Properties properties,
                                                   Map<String, Object> defaultConfig) {
    String prefix = "cluster." + clusterId + ".";

    // Required cluster-specific configs
    String bootstrapServers = properties.getProperty(prefix + "bootstrap.servers");
    String zookeeperConnect = properties.getProperty(prefix + "zookeeper.connect");

    if (bootstrapServers == null) {
      throw new IllegalArgumentException(
          "Missing required configuration: " + prefix + "bootstrap.servers");
    }
    if (zookeeperConnect == null) {
      throw new IllegalArgumentException(
          "Missing required configuration: " + prefix + "zookeeper.connect");
    }

    // Optional cluster-specific configs
    String displayName = properties.getProperty(prefix + "display.name", clusterId);
    String region = properties.getProperty(prefix + "region");
    String environment = properties.getProperty(prefix + "environment");
    boolean enabled = Boolean.parseBoolean(
        properties.getProperty(prefix + "enabled", "true"));

    // Merge default config with cluster-specific overrides
    Map<String, Object> configOverrides = new HashMap<>(defaultConfig);

    // Add cluster-specific overrides
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith(prefix)) {
        // Remove cluster prefix (e.g., "cluster.us-east." -> "")
        String configKey = key.substring(prefix.length());

        // Skip special keys that are already handled
        if (!configKey.equals("bootstrap.servers")
            && !configKey.equals("zookeeper.connect")
            && !configKey.equals("display.name")
            && !configKey.equals("region")
            && !configKey.equals("environment")
            && !configKey.equals("enabled")) {
          configOverrides.put(configKey, properties.getProperty(key));
        }
      }
    }

    return ClusterConfig.builder(clusterId)
        .bootstrapServers(bootstrapServers)
        .zookeeperConnect(zookeeperConnect)
        .displayName(displayName)
        .region(region)
        .environment(environment)
        .enabled(enabled)
        .configOverrides(configOverrides)
        .build();
  }

  /**
   * Loads a ClusterRegistry and creates a MultiClusterCoordinator from properties.
   *
   * @param properties The properties containing multi-cluster configuration
   * @param metricRegistry The metric registry for monitoring
   * @return A configured MultiClusterCoordinator
   */
  public static MultiClusterCoordinator loadCoordinator(Properties properties,
                                                        com.codahale.metrics.MetricRegistry metricRegistry) {
    ClusterRegistry registry = loadClusters(properties);
    MultiClusterCoordinator coordinator = new MultiClusterCoordinator(registry, metricRegistry);
    return coordinator;
  }
}
