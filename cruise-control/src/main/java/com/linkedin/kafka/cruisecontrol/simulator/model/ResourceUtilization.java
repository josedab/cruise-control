/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;

/**
 * Utilization metrics for a specific resource.
 */
public class ResourceUtilization {
  private final Resource _resource;
  private final double _max;
  private final double _average;
  private final double _p95;
  private final double _headroom;
  private final Assessment _assessment;

  /**
   * Constructor for ResourceUtilization.
   *
   * @param resource the resource type
   * @param max maximum utilization percentage
   * @param average average utilization percentage
   * @param p95 95th percentile utilization percentage
   * @param headroom available capacity percentage
   * @param assessment health assessment
   */
  public ResourceUtilization(Resource resource, double max, double average, double p95, double headroom, Assessment assessment) {
    _resource = resource;
    _max = max;
    _average = average;
    _p95 = p95;
    _headroom = headroom;
    _assessment = assessment;
  }

  public Resource resource() {
    return _resource;
  }

  public double max() {
    return _max;
  }

  public double average() {
    return _average;
  }

  public double p95() {
    return _p95;
  }

  public double headroom() {
    return _headroom;
  }

  public Assessment assessment() {
    return _assessment;
  }

  @Override
  public String toString() {
    return String.format("ResourceUtilization{resource=%s, max=%.2f%%, avg=%.2f%%, p95=%.2f%%, headroom=%.2f%%, assessment=%s}",
        _resource, _max, _average, _p95, _headroom, _assessment);
  }
}
