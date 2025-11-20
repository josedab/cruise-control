/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Collections;
import java.util.Map;

/**
 * Analysis of cluster capacity across all resources.
 */
public class CapacityAnalysis {
  private final Map<Resource, ResourceUtilization> _utilization;

  /**
   * Constructor for CapacityAnalysis.
   *
   * @param utilization map of resource utilization by resource type
   */
  public CapacityAnalysis(Map<Resource, ResourceUtilization> utilization) {
    _utilization = Collections.unmodifiableMap(utilization);
  }

  public Map<Resource, ResourceUtilization> utilization() {
    return _utilization;
  }

  /**
   * Get utilization for a specific resource.
   *
   * @param resource the resource type
   * @return resource utilization or null if not available
   */
  public ResourceUtilization get(Resource resource) {
    return _utilization.get(resource);
  }

  /**
   * Get CPU utilization.
   *
   * @return CPU resource utilization or null if not available
   */
  public ResourceUtilization cpu() {
    return _utilization.get(Resource.CPU);
  }

  /**
   * Get disk utilization.
   *
   * @return disk resource utilization or null if not available
   */
  public ResourceUtilization disk() {
    return _utilization.get(Resource.DISK);
  }

  /**
   * Get network inbound utilization.
   *
   * @return network inbound resource utilization or null if not available
   */
  public ResourceUtilization networkInbound() {
    return _utilization.get(Resource.NW_IN);
  }

  /**
   * Get network outbound utilization.
   *
   * @return network outbound resource utilization or null if not available
   */
  public ResourceUtilization networkOutbound() {
    return _utilization.get(Resource.NW_OUT);
  }

  @Override
  public String toString() {
    return "CapacityAnalysis{utilization=" + _utilization + "}";
  }
}
