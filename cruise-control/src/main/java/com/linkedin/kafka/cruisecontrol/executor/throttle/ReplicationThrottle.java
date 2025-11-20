/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.throttle;

/**
 * Represents Kafka replication throttle configuration.
 */
public class ReplicationThrottle {
  private final long _bytesPerSecond;

  /**
   * Creates a new replication throttle.
   *
   * @param bytesPerSecond the throttle limit in bytes per second
   */
  public ReplicationThrottle(long bytesPerSecond) {
    _bytesPerSecond = bytesPerSecond;
  }

  /**
   * Gets the throttle limit in bytes per second.
   *
   * @return bytes per second
   */
  public long getBytesPerSecond() {
    return _bytesPerSecond;
  }

  /**
   * Gets the throttle limit in megabytes per second.
   *
   * @return megabytes per second
   */
  public double getMegabytesPerSecond() {
    return _bytesPerSecond / (1024.0 * 1024.0);
  }

  @Override
  public String toString() {
    return String.format("ReplicationThrottle{%.2f MB/s}", getMegabytesPerSecond());
  }
}
