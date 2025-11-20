/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;

/**
 * Specification for a broker to add in a simulation scenario.
 */
public class BrokerSpec {
  private final int _id;
  private final String _rack;
  private final BrokerCapacityInfo _capacity;

  /**
   * Constructor for BrokerSpec.
   *
   * @param id broker ID
   * @param rack rack identifier
   * @param capacity broker capacity information
   */
  public BrokerSpec(int id, String rack, BrokerCapacityInfo capacity) {
    _id = id;
    _rack = rack;
    _capacity = capacity;
  }

  public int id() {
    return _id;
  }

  public String rack() {
    return _rack;
  }

  public BrokerCapacityInfo capacity() {
    return _capacity;
  }

  @Override
  public String toString() {
    return String.format("BrokerSpec{id=%d, rack='%s', capacity=%s}", _id, _rack, _capacity);
  }
}
