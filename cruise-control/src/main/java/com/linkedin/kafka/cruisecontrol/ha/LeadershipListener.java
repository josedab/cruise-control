/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

/**
 * Listener interface for leadership change events.
 * Implementations of this interface will be notified when this instance
 * gains or loses leadership in the cluster.
 */
public interface LeadershipListener {
  /**
   * Called when this instance becomes the leader.
   * The implementation should transition to active mode and start
   * performing cluster operations.
   */
  void onLeadershipGained();

  /**
   * Called when this instance loses leadership.
   * The implementation should transition to standby mode and stop
   * performing cluster operations.
   */
  void onLeadershipLost();
}
