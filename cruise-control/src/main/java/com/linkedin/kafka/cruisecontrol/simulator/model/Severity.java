/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * Severity level for recommendations.
 */
public enum Severity {
  /** Informational recommendation */
  INFO,
  /** Low severity - should be addressed when convenient */
  LOW,
  /** Medium severity - should be addressed soon */
  MEDIUM,
  /** High severity - should be addressed promptly */
  HIGH,
  /** Critical severity - requires immediate action */
  CRITICAL
}
