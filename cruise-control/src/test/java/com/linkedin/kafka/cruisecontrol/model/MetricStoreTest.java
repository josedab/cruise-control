/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link MetricStore}
 */
public class MetricStoreTest {

  private MetricStore _metricStore;

  @Before
  public void setUp() {
    // Reset and get fresh instance
    MetricStore.reset();
    _metricStore = MetricStore.getInstance();
  }

  @After
  public void tearDown() {
    _metricStore.clear();
    MetricStore.reset();
  }

  @Test
  public void testSingletonInstance() {
    MetricStore instance1 = MetricStore.getInstance();
    MetricStore instance2 = MetricStore.getInstance();

    assertEquals("Should return same singleton instance", instance1, instance2);
  }

  @Test
  public void testPopulateFromAggregation() {
    // Prepare test data
    TopicPartition tp1 = new TopicPartition("topic1", 0);
    TopicPartition tp2 = new TopicPartition("topic2", 1);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    // Create metrics for tp1
    AggregatedMetricValues metrics1 = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp1, new ValuesAndExtrapolations(metrics1, new HashMap<>()));
    partitionToReplicaBrokers.put(tp1, Arrays.asList(1, 2, 3));

    // Create metrics for tp2
    AggregatedMetricValues metrics2 = createSampleMetrics(150.0, 250.0);
    aggregatedMetrics.put(tp2, new ValuesAndExtrapolations(metrics2, new HashMap<>()));
    partitionToReplicaBrokers.put(tp2, Arrays.asList(1, 2));

    List<Long> windows = Arrays.asList(100L, 200L, 300L);

    // Populate the store
    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    // Verify loads are stored
    assertEquals("Should have 5 loads stored (3 + 2)", 5, _metricStore.getTotalLoadsStored());

    // Verify individual loads can be retrieved
    assertTrue("Should have load for tp1 broker 1", _metricStore.hasReplicaLoad(tp1, 1));
    assertTrue("Should have load for tp1 broker 2", _metricStore.hasReplicaLoad(tp1, 2));
    assertTrue("Should have load for tp1 broker 3", _metricStore.hasReplicaLoad(tp1, 3));
    assertTrue("Should have load for tp2 broker 1", _metricStore.hasReplicaLoad(tp2, 1));
    assertTrue("Should have load for tp2 broker 2", _metricStore.hasReplicaLoad(tp2, 2));

    assertFalse("Should not have load for tp1 broker 4", _metricStore.hasReplicaLoad(tp1, 4));
    assertFalse("Should not have load for non-existent partition",
        _metricStore.hasReplicaLoad(new TopicPartition("topic3", 0), 1));
  }

  @Test
  public void testGetReplicaLoad() {
    // Prepare test data
    TopicPartition tp = new TopicPartition("topic1", 0);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(1));

    List<Long> windows = Arrays.asList(100L, 200L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    // Get load
    Load load = _metricStore.getReplicaLoad(tp, 1);

    assertNotNull("Load should not be null", load);
    assertEquals("Should have cache hit", 1, _metricStore.getCacheHits());
    assertEquals("Should have no cache misses", 0, _metricStore.getCacheMisses());
  }

  @Test
  public void testGetReplicaLoadNotFound() {
    TopicPartition tp = new TopicPartition("topic1", 0);

    // Try to get non-existent load
    Load load = _metricStore.getReplicaLoad(tp, 1);

    assertNotNull("Load should not be null (returns empty load)", load);
    assertEquals("Should have cache miss", 1, _metricStore.getCacheMisses());
    assertEquals("Should have no cache hits", 0, _metricStore.getCacheHits());
  }

  @Test
  public void testCacheHitRatio() {
    // Prepare test data
    TopicPartition tp = new TopicPartition("topic1", 0);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(1));

    List<Long> windows = Arrays.asList(100L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    // Access existing load (hit)
    _metricStore.getReplicaLoad(tp, 1);
    _metricStore.getReplicaLoad(tp, 1);

    // Access non-existing load (miss)
    _metricStore.getReplicaLoad(tp, 2);

    // Verify cache hit ratio: 2 hits / 3 total = 0.666...
    double cacheHitRatio = _metricStore.getCacheHitRatio();
    assertTrue("Cache hit ratio should be > 0.6", cacheHitRatio > 0.6);
    assertTrue("Cache hit ratio should be < 0.7", cacheHitRatio < 0.7);
  }

  @Test
  public void testClear() {
    // Prepare test data
    TopicPartition tp = new TopicPartition("topic1", 0);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(1));

    List<Long> windows = Arrays.asList(100L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    assertTrue("Should have loads stored", _metricStore.getTotalLoadsStored() > 0);

    // Clear
    _metricStore.clear();

    assertEquals("Should have no loads stored after clear", 0, _metricStore.getTotalLoadsStored());
    assertFalse("Should not have load after clear", _metricStore.hasReplicaLoad(tp, 1));
  }

  @Test
  public void testResetMetrics() {
    // Prepare test data
    TopicPartition tp = new TopicPartition("topic1", 0);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(1));

    List<Long> windows = Arrays.asList(100L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    // Access to generate metrics
    _metricStore.getReplicaLoad(tp, 1);
    _metricStore.getReplicaLoad(tp, 2);

    assertTrue("Should have cache hits", _metricStore.getCacheHits() > 0);
    assertTrue("Should have cache misses", _metricStore.getCacheMisses() > 0);

    // Reset metrics
    _metricStore.resetMetrics();

    assertEquals("Should have no cache hits after reset", 0, _metricStore.getCacheHits());
    assertEquals("Should have no cache misses after reset", 0, _metricStore.getCacheMisses());

    // Loads should still be present
    assertTrue("Should still have loads stored", _metricStore.getTotalLoadsStored() > 0);
  }

  @Test
  public void testGetStats() {
    // Prepare test data
    TopicPartition tp = new TopicPartition("topic1", 0);

    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(100.0, 200.0);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(1, 2));

    List<Long> windows = Arrays.asList(100L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);

    Map<String, Object> stats = _metricStore.getStats();

    assertNotNull("Stats should not be null", stats);
    assertTrue("Stats should contain totalLoadsStored", stats.containsKey("totalLoadsStored"));
    assertTrue("Stats should contain cacheHits", stats.containsKey("cacheHits"));
    assertTrue("Stats should contain cacheMisses", stats.containsKey("cacheMisses"));
    assertTrue("Stats should contain cacheHitRatio", stats.containsKey("cacheHitRatio"));
    assertTrue("Stats should contain loadRatio", stats.containsKey("loadRatio"));
    assertTrue("Stats should contain partitions", stats.containsKey("partitions"));

    assertEquals("Should have 2 loads", 2L, stats.get("totalLoadsStored"));
    assertEquals("Should have 1 partition", 1, stats.get("partitions"));
  }

  @Test
  public void testNullArguments() {
    // Should not crash with null arguments
    _metricStore.populateFromAggregation(null, null, null);

    assertEquals("Should have no loads stored", 0, _metricStore.getTotalLoadsStored());
  }

  /**
   * Helper method to create sample metrics for testing.
   */
  private AggregatedMetricValues createSampleMetrics(double cpuUtil, double nwOutUtil) {
    AggregatedMetricValues metrics = new AggregatedMetricValues();

    // Add CPU metric
    short cpuMetricId = KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0);
    MetricValues cpuValues = new MetricValues(1);
    cpuValues.set(0, cpuUtil);
    metrics.add(cpuMetricId, cpuValues);

    // Add NW_OUT metric
    short nwOutMetricId = KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT).get(0);
    MetricValues nwOutValues = new MetricValues(1);
    nwOutValues.set(0, nwOutUtil);
    metrics.add(nwOutMetricId, nwOutValues);

    return metrics;
  }
}
