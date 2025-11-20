/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry of managed Kafka clusters.
 *
 * <p>This class maintains the registry of all Kafka clusters that are being managed by
 * a multi-cluster Cruise Control instance. It provides methods to:
 * <ul>
 *   <li>Register and unregister clusters</li>
 *   <li>Query clusters by ID, region, or environment</li>
 *   <li>List all registered clusters</li>
 * </ul>
 *
 * <p>This class is thread-safe and can be safely accessed from multiple threads.
 *
 * <p>Example usage:
 * <pre>
 * ClusterRegistry registry = new ClusterRegistry();
 *
 * ClusterConfig usEast = ClusterConfig.builder("us-east")
 *     .bootstrapServers("kafka1:9092,kafka2:9092")
 *     .zookeeperConnect("zk1:2181,zk2:2181")
 *     .region("us-east-1")
 *     .environment("production")
 *     .build();
 *
 * registry.registerCluster(usEast);
 *
 * // Query clusters
 * ClusterConfig cluster = registry.getCluster("us-east");
 * List&lt;ClusterConfig&gt; prodClusters = registry.getClustersByEnvironment("production");
 * </pre>
 */
public class ClusterRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterRegistry.class);
  private final Map<String, ClusterConfig> _clusters;

  /**
   * Creates a new ClusterRegistry.
   */
  public ClusterRegistry() {
    this._clusters = new ConcurrentHashMap<>();
  }

  /**
   * Registers a new cluster in the registry.
   *
   * <p>If a cluster with the same ID already exists, it will be replaced with the new configuration.
   *
   * @param config The cluster configuration to register
   * @throws NullPointerException if config is null
   */
  public void registerCluster(ClusterConfig config) {
    Objects.requireNonNull(config, "Cluster configuration cannot be null");
    String clusterId = config.clusterId();

    if (_clusters.containsKey(clusterId)) {
      LOG.warn("Cluster '{}' already registered. Replacing with new configuration.", clusterId);
    } else {
      LOG.info("Registering cluster '{}' (displayName: '{}', region: '{}', environment: '{}')",
          clusterId, config.displayName(), config.region(), config.environment());
    }

    _clusters.put(clusterId, config);
  }

  /**
   * Unregisters a cluster from the registry.
   *
   * @param clusterId The unique identifier of the cluster to unregister
   * @return The ClusterConfig that was removed, or null if the cluster was not found
   */
  public ClusterConfig unregisterCluster(String clusterId) {
    ClusterConfig removed = _clusters.remove(clusterId);
    if (removed != null) {
      LOG.info("Unregistered cluster '{}'", clusterId);
    } else {
      LOG.warn("Attempted to unregister non-existent cluster '{}'", clusterId);
    }
    return removed;
  }

  /**
   * Gets the configuration for a specific cluster.
   *
   * @param clusterId The unique identifier of the cluster
   * @return The ClusterConfig for the specified cluster, or null if not found
   */
  public ClusterConfig getCluster(String clusterId) {
    return _clusters.get(clusterId);
  }

  /**
   * Checks if a cluster is registered.
   *
   * @param clusterId The unique identifier of the cluster
   * @return true if the cluster is registered, false otherwise
   */
  public boolean containsCluster(String clusterId) {
    return _clusters.containsKey(clusterId);
  }

  /**
   * Lists all registered cluster IDs.
   *
   * @return An unmodifiable set of cluster IDs
   */
  public Set<String> listClusters() {
    return Collections.unmodifiableSet(_clusters.keySet());
  }

  /**
   * Gets all registered cluster configurations.
   *
   * @return An unmodifiable collection of all cluster configurations
   */
  public Collection<ClusterConfig> getAllClusters() {
    return Collections.unmodifiableCollection(_clusters.values());
  }

  /**
   * Gets clusters filtered by environment (e.g., "production", "staging", "dev").
   *
   * @param environment The environment to filter by
   * @return A list of clusters in the specified environment (empty list if none found)
   */
  public List<ClusterConfig> getClustersByEnvironment(String environment) {
    if (environment == null) {
      return Collections.emptyList();
    }

    return _clusters.values().stream()
        .filter(c -> environment.equals(c.environment()))
        .collect(Collectors.toList());
  }

  /**
   * Gets clusters filtered by region (e.g., "us-east-1", "eu-west-1").
   *
   * @param region The region to filter by
   * @return A list of clusters in the specified region (empty list if none found)
   */
  public List<ClusterConfig> getClustersByRegion(String region) {
    if (region == null) {
      return Collections.emptyList();
    }

    return _clusters.values().stream()
        .filter(c -> region.equals(c.region()))
        .collect(Collectors.toList());
  }

  /**
   * Gets only enabled clusters.
   *
   * @return A list of enabled clusters (empty list if none found)
   */
  public List<ClusterConfig> getEnabledClusters() {
    return _clusters.values().stream()
        .filter(ClusterConfig::enabled)
        .collect(Collectors.toList());
  }

  /**
   * Gets the number of registered clusters.
   *
   * @return The total number of clusters in the registry
   */
  public int size() {
    return _clusters.size();
  }

  /**
   * Checks if the registry is empty.
   *
   * @return true if no clusters are registered, false otherwise
   */
  public boolean isEmpty() {
    return _clusters.isEmpty();
  }

  /**
   * Clears all clusters from the registry.
   */
  public void clear() {
    LOG.info("Clearing all {} clusters from registry", _clusters.size());
    _clusters.clear();
  }

  @Override
  public String toString() {
    return "ClusterRegistry{clusterCount=" + _clusters.size()
        + ", clusters=" + _clusters.keySet()
        + '}';
  }
}
