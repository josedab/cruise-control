/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.EMPTY_BROKER_CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for lazy loading behavior in {@link Replica}
 */
public class ReplicaLazyLoadingTest {

  private static final String RACK_ID = "rack1";
  private static final String HOST = "host1";
  private static final int BROKER_ID = 1;

  private Broker _broker;
  private MetricStore _metricStore;
  private boolean _originalLazyLoadingState;

  @Before
  public void setUp() {
    // Save original lazy loading state
    _originalLazyLoadingState = Replica.isLazyLoadingEnabled();

    // Create broker
    BrokerCapacityInfo capacity = new BrokerCapacityInfo(EMPTY_BROKER_CAPACITY);
    _broker = new Broker(null, BROKER_ID, new Rack(RACK_ID), capacity);

    // Reset and get MetricStore
    MetricStore.reset();
    _metricStore = MetricStore.getInstance();
  }

  @After
  public void tearDown() {
    // Restore original lazy loading state
    Replica.setLazyLoadingEnabled(_originalLazyLoadingState);

    // Clean up MetricStore
    _metricStore.clear();
    MetricStore.reset();
  }

  @Test
  public void testLazyLoadingDisabled() {
    // Disable lazy loading
    Replica.setLazyLoadingEnabled(false);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Load should be created immediately when lazy loading is disabled
    assertNotNull("Load should not be null", replica.load());
    assertTrue("Replica should be loaded", replica.isLoaded());
  }

  @Test
  public void testLazyLoadingEnabled() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Load should not be loaded initially
    assertFalse("Replica should not be loaded initially", replica.isLoaded());

    // Populate MetricStore
    populateMetricStore(tp, BROKER_ID, 100.0);

    // Access load to trigger lazy loading
    Load load = replica.load();

    assertNotNull("Load should not be null after access", load);
    assertTrue("Replica should be loaded after first access", replica.isLoaded());
  }

  @Test
  public void testLazyLoadingFetchesFromMetricStore() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Populate MetricStore with specific metrics
    double cpuUtil = 75.5;
    populateMetricStore(tp, BROKER_ID, cpuUtil);

    // Access load to trigger lazy loading
    Load load = replica.load();

    assertNotNull("Load should not be null", load);

    // Verify the load was fetched from MetricStore
    // The MetricStore should have been accessed
    assertTrue("MetricStore should have cache hits", _metricStore.getCacheHits() > 0);
  }

  @Test
  public void testLazyLoadingWithoutMetricStore() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Don't populate MetricStore

    // Access load - should return empty load
    Load load = replica.load();

    assertNotNull("Load should not be null (returns empty load)", load);
    assertTrue("Replica should be loaded", replica.isLoaded());

    // Should have a cache miss
    assertTrue("MetricStore should have cache misses", _metricStore.getCacheMisses() > 0);
  }

  @Test
  public void testSetMetricValuesWithLazyLoading() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Set metric values directly
    AggregatedMetricValues metrics = createSampleMetrics(50.0);
    List<Long> windows = Arrays.asList(100L);
    replica.setMetricValues(metrics, windows);

    // Should be loaded now
    assertTrue("Replica should be loaded after setMetricValues", replica.isLoaded());

    // Load should be accessible
    Load load = replica.load();
    assertNotNull("Load should not be null", load);
  }

  @Test
  public void testClearLoadWithLazyLoading() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, true);

    // Clear load
    replica.clearLoad();

    // Should be loaded after clearing
    assertTrue("Replica should be loaded after clearLoad", replica.isLoaded());
  }

  @Test
  public void testLazyLoadingStateManagement() {
    // Test state management
    assertFalse("Lazy loading should be disabled by default", Replica.isLazyLoadingEnabled());

    Replica.setLazyLoadingEnabled(true);
    assertTrue("Lazy loading should be enabled", Replica.isLazyLoadingEnabled());

    Replica.setLazyLoadingEnabled(false);
    assertFalse("Lazy loading should be disabled", Replica.isLazyLoadingEnabled());
  }

  @Test
  public void testMultipleReplicasLazyLoading() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp1 = new TopicPartition(TOPIC0, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC0, 1);

    Replica replica1 = new Replica(tp1, _broker, true);
    Replica replica2 = new Replica(tp2, _broker, false);

    // Populate MetricStore for both
    populateMetricStore(tp1, BROKER_ID, 100.0);
    populateMetricStore(tp2, BROKER_ID, 150.0);

    // Access only replica1's load
    replica1.load();

    // replica1 should be loaded
    assertTrue("Replica1 should be loaded", replica1.isLoaded());

    // replica2 should not be loaded yet
    assertFalse("Replica2 should not be loaded", replica2.isLoaded());

    // Now access replica2's load
    replica2.load();

    // Both should be loaded
    assertTrue("Replica1 should still be loaded", replica1.isLoaded());
    assertTrue("Replica2 should be loaded", replica2.isLoaded());
  }

  @Test
  public void testLazyLoadingWithMakeLeader() {
    // Enable lazy loading
    Replica.setLazyLoadingEnabled(true);

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    Replica replica = new Replica(tp, _broker, false);

    // Populate MetricStore
    populateMetricStore(tp, BROKER_ID, 50.0);

    // Make leader - this should trigger lazy loading
    AggregatedMetricValues leadershipLoad = createSampleMetrics(10.0);
    replica.makeLeader(leadershipLoad);

    assertTrue("Replica should be loaded after makeLeader", replica.isLoaded());
  }

  /**
   * Helper method to populate MetricStore with sample data.
   */
  private void populateMetricStore(TopicPartition tp, int brokerId, double cpuUtil) {
    Map<TopicPartition, ValuesAndExtrapolations> aggregatedMetrics = new HashMap<>();
    Map<TopicPartition, List<Integer>> partitionToReplicaBrokers = new HashMap<>();

    AggregatedMetricValues metrics = createSampleMetrics(cpuUtil);
    aggregatedMetrics.put(tp, new ValuesAndExtrapolations(metrics, new HashMap<>()));
    partitionToReplicaBrokers.put(tp, Arrays.asList(brokerId));

    List<Long> windows = Arrays.asList(100L);

    _metricStore.populateFromAggregation(aggregatedMetrics, partitionToReplicaBrokers, windows);
  }

  /**
   * Helper method to create sample metrics for testing.
   */
  private AggregatedMetricValues createSampleMetrics(double cpuUtil) {
    AggregatedMetricValues metrics = new AggregatedMetricValues();

    // Add CPU metric
    short cpuMetricId = KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0);
    MetricValues cpuValues = new MetricValues(1);
    cpuValues.set(0, cpuUtil);
    metrics.add(cpuMetricId, cpuValues);

    return metrics;
  }
}
