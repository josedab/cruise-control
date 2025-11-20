/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.throttle;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ThrottleManager}.
 */
public class ThrottleManagerTest {

  private ThrottleManager _throttleManager;
  private static final long BANDWIDTH_LIMIT = 1_000_000; // 1 MB/s

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, "10");
    props.setProperty(NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "5");
    props.setProperty(NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG, "2");
    props.setProperty(NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG, "100");
    props.setProperty(MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG, "10");
    props.setProperty(NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG, "10");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    ExecutionConcurrencyManager concurrencyManager = new ExecutionConcurrencyManager(config);

    _throttleManager = new ThrottleManager(concurrencyManager, BANDWIDTH_LIMIT);
  }

  @Test
  public void testRequestCapacity() {
    // Initially, should be able to get up to max
    int granted = _throttleManager.requestInterBrokerCapacity(5);
    assertEquals(5, granted);
    assertEquals(5, _throttleManager.getOngoingInterBrokerMovements());

    // Can get more up to limit
    granted = _throttleManager.requestInterBrokerCapacity(3);
    assertEquals(3, granted);
    assertEquals(8, _throttleManager.getOngoingInterBrokerMovements());

    // Requesting more than available should only grant what's available
    granted = _throttleManager.requestInterBrokerCapacity(10);
    assertEquals(2, granted); // Only 2 left (max is 10)
    assertEquals(10, _throttleManager.getOngoingInterBrokerMovements());

    // No more capacity available
    granted = _throttleManager.requestInterBrokerCapacity(5);
    assertEquals(0, granted);
  }

  @Test
  public void testReleaseCapacity() {
    // Request capacity
    _throttleManager.requestInterBrokerCapacity(5);
    assertEquals(5, _throttleManager.getOngoingInterBrokerMovements());

    // Release some
    _throttleManager.releaseInterBrokerCapacity(2);
    assertEquals(3, _throttleManager.getOngoingInterBrokerMovements());

    // Can request again
    int granted = _throttleManager.requestInterBrokerCapacity(5);
    assertEquals(5, granted);
    assertEquals(8, _throttleManager.getOngoingInterBrokerMovements());
  }

  @Test
  public void testAvailableCapacity() {
    assertEquals(10, _throttleManager.availableInterBrokerCapacity());

    _throttleManager.requestInterBrokerCapacity(7);
    assertEquals(3, _throttleManager.availableInterBrokerCapacity());

    _throttleManager.releaseInterBrokerCapacity(2);
    assertEquals(5, _throttleManager.availableInterBrokerCapacity());
  }

  @Test
  public void testIsThrottled() {
    assertFalse(_throttleManager.isThrottled(ConcurrencyType.INTER_BROKER_REPLICA));

    // Fill up capacity
    _throttleManager.requestInterBrokerCapacity(10);
    assertTrue(_throttleManager.isThrottled(ConcurrencyType.INTER_BROKER_REPLICA));

    // Release some
    _throttleManager.releaseInterBrokerCapacity(1);
    assertFalse(_throttleManager.isThrottled(ConcurrencyType.INTER_BROKER_REPLICA));
  }

  @Test
  public void testLeadershipCapacity() {
    // Leadership has higher limit (100)
    int granted = _throttleManager.requestLeadershipCapacity(50);
    assertEquals(50, granted);

    granted = _throttleManager.requestLeadershipCapacity(60);
    assertEquals(50, granted); // Only 50 left

    _throttleManager.releaseLeadershipCapacity(30);
    granted = _throttleManager.requestLeadershipCapacity(40);
    assertEquals(30, granted); // 30 released + 50 remaining - 50 in use = 30
  }

  @Test
  public void testGetKafkaThrottle() {
    ReplicationThrottle throttle = _throttleManager.getKafkaThrottle();
    assertNotNull(throttle);
    assertEquals(BANDWIDTH_LIMIT, throttle.getBytesPerSecond());
  }

  @Test
  public void testReset() {
    _throttleManager.requestInterBrokerCapacity(5);
    _throttleManager.requestIntraBrokerCapacity(2);
    _throttleManager.requestLeadershipCapacity(10);

    assertEquals(5, _throttleManager.getOngoingInterBrokerMovements());
    assertEquals(2, _throttleManager.getOngoingIntraBrokerMovements());
    assertEquals(10, _throttleManager.getOngoingLeadershipMovements());

    _throttleManager.reset();

    assertEquals(0, _throttleManager.getOngoingInterBrokerMovements());
    assertEquals(0, _throttleManager.getOngoingIntraBrokerMovements());
    assertEquals(0, _throttleManager.getOngoingLeadershipMovements());
  }

  @Test
  public void testConcurrentRequests() throws InterruptedException {
    // Test that concurrent requests don't exceed limit
    final int numThreads = 20;
    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(() -> {
        _throttleManager.requestInterBrokerCapacity(1);
      });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // Should never exceed max capacity
    assertTrue(_throttleManager.getOngoingInterBrokerMovements() <= 10);
  }
}
