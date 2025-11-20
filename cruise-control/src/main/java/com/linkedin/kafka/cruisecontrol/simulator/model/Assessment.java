/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * Assessment of resource capacity health.
 */
public enum Assessment {
  /** Capacity is healthy with ample headroom (>30%) */
  HEALTHY,
  /** Capacity is adequate for current load (15-30% headroom) */
  ADEQUATE,
  /** Capacity is marginal and should be monitored (5-15% headroom) */
  MARGINAL,
  /** Capacity is insufficient and requires immediate action (<5% headroom) */
  INSUFFICIENT
}
