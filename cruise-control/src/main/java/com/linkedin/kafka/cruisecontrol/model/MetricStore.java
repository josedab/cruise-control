/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A centralized store for replica metric data that enables lazy loading of replica loads.
 * This class serves as an in-memory cache of metrics organized by TopicPartition and BrokerId
 * for efficient access during goal optimization.
 *
 * <p>The MetricStore is populated during LoadMonitor aggregation and provides thread-safe
 * access to replica load information. It tracks metrics for monitoring memory usage and
 * cache efficiency.</p>
 */
public class MetricStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetricStore.class);

  // Singleton instance
  private static volatile MetricStore _instance;

  // In-memory cache of partition metrics: TopicPartition -> BrokerId -> Load
  private final Map<TopicPartition, Map<Integer, Load>> _replicaLoads;

  // Metrics for monitoring
  private final AtomicLong _cacheHits = new AtomicLong(0);
  private final AtomicLong _cacheMisses = new AtomicLong(0);
  private final AtomicLong _totalLoadsStored = new AtomicLong(0);

  /**
   * Private constructor for singleton pattern.
   */
  private MetricStore() {
    _replicaLoads = new ConcurrentHashMap<>();
  }

  /**
   * Get the singleton instance of MetricStore.
   *
   * @return The MetricStore instance.
   */
  public static MetricStore getInstance() {
    if (_instance == null) {
      synchronized (MetricStore.class) {
        if (_instance == null) {
          _instance = new MetricStore();
        }
      }
    }
    return _instance;
  }

  /**
   * Reset the singleton instance (primarily for testing).
   */
  public static void reset() {
    synchronized (MetricStore.class) {
      _instance = null;
    }
  }

  /**
   * Populate the metric store from aggregated partition metrics.
   * This method is called during LoadMonitor aggregation to pre-populate
   * the store with metric data.
   *
   * @param aggregatedMetrics Map of TopicPartition to aggregated metric values.
   * @param partitionToReplicaBrokers Map of TopicPartition to list of broker IDs hosting replicas.
   * @param windows The time windows for the aggregated metrics.
   */
  public void populateFromAggregation(
      Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics,
      Map<TopicPartition, List<Integer>> partitionToReplicaBrokers,
      List<Long> windows) {

    if (aggregatedMetrics == null || partitionToReplicaBrokers == null) {
      LOG.warn("Received null arguments in populateFromAggregation, skipping population");
      return;
    }

    long startTime = System.currentTimeMillis();
    int loadsPopulated = 0;

    // Clear existing data
    _replicaLoads.clear();
    _totalLoadsStored.set(0);

    for (Map.Entry<TopicPartition, ValuesAndExtrapolations> entry : aggregatedMetrics.entrySet()) {
      TopicPartition tp = entry.getKey();
      ValuesAndExtrapolations metrics = entry.getValue();

      // Get the brokers hosting replicas for this partition
      List<Integer> replicaBrokers = partitionToReplicaBrokers.get(tp);
      if (replicaBrokers == null || replicaBrokers.isEmpty()) {
        continue;
      }

      // Create loads for each replica
      Map<Integer, Load> brokerLoads = new ConcurrentHashMap<>();
      for (int brokerId : replicaBrokers) {
        Load load = createLoadFromMetrics(metrics, windows);
        brokerLoads.put(brokerId, load);
        loadsPopulated++;
      }

      _replicaLoads.put(tp, brokerLoads);
    }

    _totalLoadsStored.set(loadsPopulated);

    long elapsed = System.currentTimeMillis() - startTime;
    LOG.debug("Populated MetricStore with {} loads for {} partitions in {} ms",
        loadsPopulated, aggregatedMetrics.size(), elapsed);
  }

  /**
   * Create a Load object from aggregated metric values.
   *
   * @param metrics The aggregated metric values.
   * @param windows The time windows for the metrics.
   * @return A new Load object populated with the metrics.
   */
  private Load createLoadFromMetrics(ValuesAndExtrapolations metrics, List<Long> windows) {
    Load load = new Load();
    AggregatedMetricValues aggregatedValues = metrics.metricValues();
    load.initializeMetricValues(aggregatedValues, windows);
    return load;
  }

  /**
   * Get the Load for a specific replica.
   *
   * @param tp The topic partition.
   * @param brokerId The broker ID hosting the replica.
   * @return The Load for the replica, or null if not found.
   */
  public Load getReplicaLoad(TopicPartition tp, int brokerId) {
    Map<Integer, Load> brokerLoads = _replicaLoads.get(tp);

    if (brokerLoads != null) {
      Load load = brokerLoads.get(brokerId);
      if (load != null) {
        _cacheHits.incrementAndGet();
        return load;
      }
    }

    _cacheMisses.incrementAndGet();

    // If not found, return an empty Load rather than null to avoid NPE
    LOG.warn("Load not found for partition {} on broker {}, returning empty Load", tp, brokerId);
    return new Load();
  }

  /**
   * Check if the store has a load for a specific replica.
   *
   * @param tp The topic partition.
   * @param brokerId The broker ID.
   * @return true if the load exists, false otherwise.
   */
  public boolean hasReplicaLoad(TopicPartition tp, int brokerId) {
    Map<Integer, Load> brokerLoads = _replicaLoads.get(tp);
    return brokerLoads != null && brokerLoads.containsKey(brokerId);
  }

  /**
   * Clear all stored loads.
   */
  public void clear() {
    _replicaLoads.clear();
    _totalLoadsStored.set(0);
    LOG.debug("MetricStore cleared");
  }

  /**
   * Get the number of cache hits.
   *
   * @return The number of cache hits.
   */
  public long getCacheHits() {
    return _cacheHits.get();
  }

  /**
   * Get the number of cache misses.
   *
   * @return The number of cache misses.
   */
  public long getCacheMisses() {
    return _cacheMisses.get();
  }

  /**
   * Get the total number of loads stored.
   *
   * @return The total number of loads stored.
   */
  public long getTotalLoadsStored() {
    return _totalLoadsStored.get();
  }

  /**
   * Get the cache hit ratio.
   *
   * @return The cache hit ratio (0.0 to 1.0), or 0.0 if no accesses have been made.
   */
  public double getCacheHitRatio() {
    long hits = _cacheHits.get();
    long misses = _cacheMisses.get();
    long total = hits + misses;

    if (total == 0) {
      return 0.0;
    }

    return (double) hits / total;
  }

  /**
   * Get the load ratio (percentage of loads that have been accessed).
   * This metric helps track how much of the stored data is actually being used.
   *
   * @return The load ratio (0.0 to 1.0), or 0.0 if no loads are stored.
   */
  public double getLoadRatio() {
    long totalStored = _totalLoadsStored.get();
    long totalAccessed = _cacheHits.get(); // Only count successful accesses

    if (totalStored == 0) {
      return 0.0;
    }

    // Note: This is an approximation since a single replica can be accessed multiple times
    // For accurate tracking, we would need to maintain a set of accessed replica keys
    return Math.min(1.0, (double) totalAccessed / totalStored);
  }

  /**
   * Reset all metrics (primarily for testing).
   */
  public void resetMetrics() {
    _cacheHits.set(0);
    _cacheMisses.set(0);
  }

  /**
   * Get statistics about the metric store.
   *
   * @return A map of statistic names to values.
   */
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new ConcurrentHashMap<>();
    stats.put("totalLoadsStored", _totalLoadsStored.get());
    stats.put("cacheHits", _cacheHits.get());
    stats.put("cacheMisses", _cacheMisses.get());
    stats.put("cacheHitRatio", getCacheHitRatio());
    stats.put("loadRatio", getLoadRatio());
    stats.put("partitions", _replicaLoads.size());
    return stats;
  }
}
