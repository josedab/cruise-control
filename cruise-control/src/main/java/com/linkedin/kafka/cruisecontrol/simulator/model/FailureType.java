/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * Types of failures that can be simulated.
 */
public enum FailureType {
  /** Broker is completely dead/unavailable */
  BROKER_DEAD,
  /** Broker disk is full */
  DISK_FULL,
  /** Broker is slow/experiencing high latency */
  SLOW_BROKER,
  /** Entire rack has failed */
  RACK_FAILURE
}
