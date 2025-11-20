/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates operations across multiple Kafka clusters.
 *
 * <p>This is the central orchestrator for multi-cluster management. It:
 * <ul>
 *   <li>Maintains a registry of all managed clusters</li>
 *   <li>Creates and manages ClusterContext for each cluster</li>
 *   <li>Provides methods to execute operations on all clusters or a subset</li>
 *   <li>Aggregates metrics and health status across clusters</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * ClusterRegistry registry = new ClusterRegistry();
 * registry.registerCluster(usEastConfig);
 * registry.registerCluster(usWestConfig);
 *
 * MultiClusterCoordinator coordinator = new MultiClusterCoordinator(registry, metricRegistry);
 * coordinator.initialize();
 *
 * // Execute on all clusters
 * Map&lt;String, ClusterHealth&gt; health = coordinator.getAllHealth();
 *
 * // Execute on specific cluster
 * ClusterContext context = coordinator.getContext("us-east");
 * context.pauseSampling();
 * </pre>
 */
public class MultiClusterCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(MultiClusterCoordinator.class);

  private final ClusterRegistry _registry;
  private final MetricRegistry _metricRegistry;
  private final Map<String, ClusterContext> _contexts;
  private volatile boolean _initialized = false;

  /**
   * Creates a new MultiClusterCoordinator.
   *
   * @param registry The cluster registry containing cluster configurations
   * @param metricRegistry The metric registry for monitoring
   * @throws NullPointerException if registry or metricRegistry is null
   */
  public MultiClusterCoordinator(ClusterRegistry registry, MetricRegistry metricRegistry) {
    this._registry = Objects.requireNonNull(registry, "Cluster registry cannot be null");
    this._metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry cannot be null");
    this._contexts = new ConcurrentHashMap<>();
  }

  /**
   * Initializes all registered clusters and starts their contexts.
   *
   * <p>This creates a ClusterContext for each enabled cluster in the registry
   * and starts monitoring.
   *
   * @throws IllegalStateException if already initialized
   */
  public synchronized void initialize() {
    if (_initialized) {
      throw new IllegalStateException("MultiClusterCoordinator already initialized");
    }

    LOG.info("Initializing MultiClusterCoordinator with {} registered clusters", _registry.size());

    List<ClusterConfig> enabledClusters = _registry.getEnabledClusters();
    LOG.info("Starting {} enabled clusters", enabledClusters.size());

    List<String> successfullyStarted = new ArrayList<>();
    List<String> failedToStart = new ArrayList<>();

    for (ClusterConfig config : enabledClusters) {
      String clusterId = config.clusterId();
      try {
        LOG.info("Initializing cluster '{}'", clusterId);
        ClusterContext context = new ClusterContext(config, _metricRegistry);
        context.startUp();
        _contexts.put(clusterId, context);
        successfullyStarted.add(clusterId);
        LOG.info("Successfully initialized cluster '{}'", clusterId);
      } catch (Exception e) {
        LOG.error("Failed to initialize cluster '{}'", clusterId, e);
        failedToStart.add(clusterId);
      }
    }

    _initialized = true;

    LOG.info("MultiClusterCoordinator initialization complete. "
        + "Successfully started: {}, Failed: {}",
        successfullyStarted.size(), failedToStart.size());

    if (!failedToStart.isEmpty()) {
      LOG.warn("Failed to start clusters: {}", failedToStart);
    }
  }

  /**
   * Shuts down all cluster contexts.
   */
  public synchronized void shutdown() {
    if (!_initialized) {
      LOG.warn("MultiClusterCoordinator not initialized, nothing to shutdown");
      return;
    }

    LOG.info("Shutting down MultiClusterCoordinator with {} cluster contexts", _contexts.size());

    for (Map.Entry<String, ClusterContext> entry : _contexts.entrySet()) {
      try {
        LOG.info("Shutting down cluster '{}'", entry.getKey());
        entry.getValue().shutdown();
      } catch (Exception e) {
        LOG.error("Error shutting down cluster '{}'", entry.getKey(), e);
      }
    }

    _contexts.clear();
    _initialized = false;
    LOG.info("MultiClusterCoordinator shutdown complete");
  }

  /**
   * Gets the cluster context for a specific cluster.
   *
   * @param clusterId The unique identifier of the cluster
   * @return The ClusterContext for the specified cluster, or null if not found
   */
  public ClusterContext getContext(String clusterId) {
    return _contexts.get(clusterId);
  }

  /**
   * Gets all cluster contexts.
   *
   * @return An unmodifiable map of cluster IDs to contexts
   */
  public Map<String, ClusterContext> getAllContexts() {
    return Collections.unmodifiableMap(_contexts);
  }

  /**
   * Gets the cluster registry.
   *
   * @return The cluster registry
   */
  public ClusterRegistry registry() {
    return _registry;
  }

  /**
   * Executes an operation on all clusters in parallel.
   *
   * @param operation The operation to perform on each cluster
   * @param <T> The return type of the operation
   * @return A map of cluster IDs to operation results
   */
  public <T> Map<String, T> executeOnAll(Function<ClusterContext, T> operation) {
    return _contexts.entrySet().parallelStream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> {
              try {
                return operation.apply(entry.getValue());
              } catch (Exception e) {
                LOG.error("Error executing operation on cluster '{}'", entry.getKey(), e);
                return null;
              }
            }
        ));
  }

  /**
   * Executes an operation on specific clusters in parallel.
   *
   * @param clusterIds The IDs of clusters to operate on
   * @param operation The operation to perform on each cluster
   * @param <T> The return type of the operation
   * @return A map of cluster IDs to operation results
   */
  public <T> Map<String, T> executeOnClusters(List<String> clusterIds,
                                               Function<ClusterContext, T> operation) {
    return clusterIds.parallelStream()
        .filter(_contexts::containsKey)
        .collect(Collectors.toMap(
            clusterId -> clusterId,
            clusterId -> {
              try {
                ClusterContext context = _contexts.get(clusterId);
                return operation.apply(context);
              } catch (Exception e) {
                LOG.error("Error executing operation on cluster '{}'", clusterId, e);
                return null;
              }
            }
        ));
  }

  /**
   * Gets health status for all clusters.
   *
   * @return A map of cluster IDs to health status
   */
  public Map<String, ClusterContext.ClusterHealth> getAllHealth() {
    LOG.debug("Getting health for all {} clusters", _contexts.size());
    return executeOnAll(ClusterContext::health);
  }

  /**
   * Gets an aggregated health summary across all clusters.
   *
   * @return An AggregatedHealth object with summary statistics
   */
  public AggregatedHealth getAggregatedHealth() {
    Map<String, ClusterContext.ClusterHealth> allHealth = getAllHealth();

    int total = allHealth.size();
    int healthy = 0;
    int degraded = 0;
    int rebalancing = 0;
    int error = 0;

    for (ClusterContext.ClusterHealth health : allHealth.values()) {
      String status = health.status();
      switch (status) {
        case "HEALTHY":
          healthy++;
          break;
        case "DEGRADED":
          degraded++;
          break;
        case "REBALANCING":
          rebalancing++;
          break;
        case "ERROR":
          error++;
          break;
      }
    }

    return new AggregatedHealth(total, healthy, degraded, rebalancing, error, allHealth);
  }

  /**
   * Checks if the coordinator has been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public boolean isInitialized() {
    return _initialized;
  }

  /**
   * Gets the number of managed clusters.
   *
   * @return The number of cluster contexts
   */
  public int clusterCount() {
    return _contexts.size();
  }

  @Override
  public String toString() {
    return "MultiClusterCoordinator{"
        + "clusterCount=" + _contexts.size()
        + ", initialized=" + _initialized
        + ", clusters=" + _contexts.keySet()
        + '}';
  }

  /**
   * Aggregated health information across all clusters.
   */
  public static class AggregatedHealth {
    private final int _totalClusters;
    private final int _healthyClusters;
    private final int _degradedClusters;
    private final int _rebalancingClusters;
    private final int _errorClusters;
    private final Map<String, ClusterContext.ClusterHealth> _perClusterHealth;

    public AggregatedHealth(int totalClusters,
                            int healthyClusters,
                            int degradedClusters,
                            int rebalancingClusters,
                            int errorClusters,
                            Map<String, ClusterContext.ClusterHealth> perClusterHealth) {
      this._totalClusters = totalClusters;
      this._healthyClusters = healthyClusters;
      this._degradedClusters = degradedClusters;
      this._rebalancingClusters = rebalancingClusters;
      this._errorClusters = errorClusters;
      this._perClusterHealth = new HashMap<>(perClusterHealth);
    }

    public int totalClusters() {
      return _totalClusters;
    }

    public int healthyClusters() {
      return _healthyClusters;
    }

    public int degradedClusters() {
      return _degradedClusters;
    }

    public int rebalancingClusters() {
      return _rebalancingClusters;
    }

    public int errorClusters() {
      return _errorClusters;
    }

    public Map<String, ClusterContext.ClusterHealth> perClusterHealth() {
      return Collections.unmodifiableMap(_perClusterHealth);
    }

    @Override
    public String toString() {
      return "AggregatedHealth{"
          + "total=" + _totalClusters
          + ", healthy=" + _healthyClusters
          + ", degraded=" + _degradedClusters
          + ", rebalancing=" + _rebalancingClusters
          + ", error=" + _errorClusters
          + '}';
    }
  }

  /**
   * Aggregated metrics across all clusters.
   */
  public static class AggregatedMetrics {
    private final int _totalBrokers;
    private final int _totalPartitions;
    private final int _ongoingRebalances;
    private final Map<String, ClusterMetrics> _perClusterMetrics;

    public AggregatedMetrics(int totalBrokers,
                             int totalPartitions,
                             int ongoingRebalances,
                             Map<String, ClusterMetrics> perClusterMetrics) {
      this._totalBrokers = totalBrokers;
      this._totalPartitions = totalPartitions;
      this._ongoingRebalances = ongoingRebalances;
      this._perClusterMetrics = new HashMap<>(perClusterMetrics);
    }

    public int totalBrokers() {
      return _totalBrokers;
    }

    public int totalPartitions() {
      return _totalPartitions;
    }

    public int ongoingRebalances() {
      return _ongoingRebalances;
    }

    public Map<String, ClusterMetrics> perClusterMetrics() {
      return Collections.unmodifiableMap(_perClusterMetrics);
    }

    @Override
    public String toString() {
      return "AggregatedMetrics{"
          + "totalBrokers=" + _totalBrokers
          + ", totalPartitions=" + _totalPartitions
          + ", ongoingRebalances=" + _ongoingRebalances
          + '}';
    }
  }

  /**
   * Metrics for a single cluster.
   */
  public static class ClusterMetrics {
    private final String _clusterId;
    private final int _brokers;
    private final int _partitions;
    private final boolean _rebalancing;

    public ClusterMetrics(String clusterId, int brokers, int partitions, boolean rebalancing) {
      this._clusterId = clusterId;
      this._brokers = brokers;
      this._partitions = partitions;
      this._rebalancing = rebalancing;
    }

    public String clusterId() {
      return _clusterId;
    }

    public int brokers() {
      return _brokers;
    }

    public int partitions() {
      return _partitions;
    }

    public boolean rebalancing() {
      return _rebalancing;
    }

    @Override
    public String toString() {
      return "ClusterMetrics{"
          + "clusterId='" + _clusterId + '\''
          + ", brokers=" + _brokers
          + ", partitions=" + _partitions
          + ", rebalancing=" + _rebalancing
          + '}';
    }
  }
}
