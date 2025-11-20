/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.throttle;

import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages throttling of partition movements to prevent cluster overload.
 *
 * <p>Supports two types of throttling:
 * <ul>
 *   <li><b>Concurrency throttling:</b> Limits number of simultaneous movements</li>
 *   <li><b>Bandwidth throttling:</b> Limits replication bandwidth (via Kafka)</li>
 * </ul>
 *
 * <p>This class works in conjunction with {@link ExecutionConcurrencyManager} to
 * enforce both cluster-wide and per-broker concurrency limits.
 */
public class ThrottleManager {
  private static final Logger LOG = LoggerFactory.getLogger(ThrottleManager.class);

  private final ExecutionConcurrencyManager _concurrencyManager;
  private final long _maxBandwidthBytesPerSec;

  // Track ongoing movements by type
  private final AtomicInteger _ongoingInterBrokerMovements = new AtomicInteger(0);
  private final AtomicInteger _ongoingIntraBrokerMovements = new AtomicInteger(0);
  private final AtomicInteger _ongoingLeadershipMovements = new AtomicInteger(0);

  /**
   * Creates a new ThrottleManager.
   *
   * @param concurrencyManager the concurrency manager
   * @param maxBandwidthBytesPerSec maximum bandwidth for replication in bytes/sec
   */
  public ThrottleManager(ExecutionConcurrencyManager concurrencyManager,
                        long maxBandwidthBytesPerSec) {
    _concurrencyManager = concurrencyManager;
    _maxBandwidthBytesPerSec = maxBandwidthBytesPerSec;
  }

  /**
   * Determines how many more inter-broker tasks can be submitted given current throttle.
   *
   * @return number of additional tasks that can be submitted
   */
  public int availableInterBrokerCapacity() {
    int maxAllowed = _concurrencyManager.maxClusterInterBrokerPartitionMovements();
    int ongoing = _ongoingInterBrokerMovements.get();
    return Math.max(0, maxAllowed - ongoing);
  }

  /**
   * Determines how many more intra-broker tasks can be submitted given current throttle.
   *
   * @return number of additional tasks that can be submitted
   */
  public int availableIntraBrokerCapacity() {
    // Intra-broker movements are typically limited per-broker
    // For simplicity, we use a cluster-wide limit here
    int maxAllowed = _concurrencyManager.maxClusterInterBrokerPartitionMovements();
    int ongoing = _ongoingIntraBrokerMovements.get();
    return Math.max(0, maxAllowed - ongoing);
  }

  /**
   * Determines how many more leadership tasks can be submitted given current throttle.
   *
   * @return number of additional tasks that can be submitted
   */
  public int availableLeadershipCapacity() {
    int maxAllowed = _concurrencyManager.clusterLeadershipMovementConcurrency();
    int ongoing = _ongoingLeadershipMovements.get();
    return Math.max(0, maxAllowed - ongoing);
  }

  /**
   * Requests capacity to execute inter-broker tasks.
   *
   * @param requestedTasks number of tasks wanting to start
   * @return number of tasks that can actually start (may be less)
   */
  public int requestInterBrokerCapacity(int requestedTasks) {
    int available = availableInterBrokerCapacity();
    int granted = Math.min(requestedTasks, available);

    if (granted > 0) {
      int newCount = _ongoingInterBrokerMovements.addAndGet(granted);
      LOG.debug("Granted {} inter-broker task slots, {} now in progress", granted, newCount);
    }

    return granted;
  }

  /**
   * Requests capacity to execute intra-broker tasks.
   *
   * @param requestedTasks number of tasks wanting to start
   * @return number of tasks that can actually start (may be less)
   */
  public int requestIntraBrokerCapacity(int requestedTasks) {
    int available = availableIntraBrokerCapacity();
    int granted = Math.min(requestedTasks, available);

    if (granted > 0) {
      int newCount = _ongoingIntraBrokerMovements.addAndGet(granted);
      LOG.debug("Granted {} intra-broker task slots, {} now in progress", granted, newCount);
    }

    return granted;
  }

  /**
   * Requests capacity to execute leadership tasks.
   *
   * @param requestedTasks number of tasks wanting to start
   * @return number of tasks that can actually start (may be less)
   */
  public int requestLeadershipCapacity(int requestedTasks) {
    int available = availableLeadershipCapacity();
    int granted = Math.min(requestedTasks, available);

    if (granted > 0) {
      int newCount = _ongoingLeadershipMovements.addAndGet(granted);
      LOG.debug("Granted {} leadership task slots, {} now in progress", granted, newCount);
    }

    return granted;
  }

  /**
   * Releases inter-broker capacity when tasks complete.
   *
   * @param completedTasks number of tasks that finished
   */
  public void releaseInterBrokerCapacity(int completedTasks) {
    if (completedTasks > 0) {
      int newCount = _ongoingInterBrokerMovements.addAndGet(-completedTasks);
      LOG.debug("Released {} inter-broker task slots, {} now in progress", completedTasks, newCount);
    }
  }

  /**
   * Releases intra-broker capacity when tasks complete.
   *
   * @param completedTasks number of tasks that finished
   */
  public void releaseIntraBrokerCapacity(int completedTasks) {
    if (completedTasks > 0) {
      int newCount = _ongoingIntraBrokerMovements.addAndGet(-completedTasks);
      LOG.debug("Released {} intra-broker task slots, {} now in progress", completedTasks, newCount);
    }
  }

  /**
   * Releases leadership capacity when tasks complete.
   *
   * @param completedTasks number of tasks that finished
   */
  public void releaseLeadershipCapacity(int completedTasks) {
    if (completedTasks > 0) {
      int newCount = _ongoingLeadershipMovements.addAndGet(-completedTasks);
      LOG.debug("Released {} leadership task slots, {} now in progress", completedTasks, newCount);
    }
  }

  /**
   * Gets Kafka replication throttle configuration.
   *
   * @return throttle in bytes per second
   */
  public ReplicationThrottle getKafkaThrottle() {
    return new ReplicationThrottle(_maxBandwidthBytesPerSec);
  }

  /**
   * Gets the number of ongoing inter-broker movements.
   *
   * @return count of ongoing movements
   */
  public int getOngoingInterBrokerMovements() {
    return _ongoingInterBrokerMovements.get();
  }

  /**
   * Gets the number of ongoing intra-broker movements.
   *
   * @return count of ongoing movements
   */
  public int getOngoingIntraBrokerMovements() {
    return _ongoingIntraBrokerMovements.get();
  }

  /**
   * Gets the number of ongoing leadership movements.
   *
   * @return count of ongoing movements
   */
  public int getOngoingLeadershipMovements() {
    return _ongoingLeadershipMovements.get();
  }

  /**
   * Resets all counters to zero.
   */
  public void reset() {
    _ongoingInterBrokerMovements.set(0);
    _ongoingIntraBrokerMovements.set(0);
    _ongoingLeadershipMovements.set(0);
    LOG.info("Throttle manager reset, all counters cleared");
  }

  /**
   * Checks if throttling is currently active for a given concurrency type.
   *
   * @param type the concurrency type to check
   * @return true if throttle is active (at or near limit)
   */
  public boolean isThrottled(ConcurrencyType type) {
    return switch (type) {
      case INTER_BROKER_REPLICA -> availableInterBrokerCapacity() == 0;
      case INTRA_BROKER_REPLICA -> availableIntraBrokerCapacity() == 0;
      case LEADERSHIP_CLUSTER, LEADERSHIP_BROKER -> availableLeadershipCapacity() == 0;
    };
  }
}
