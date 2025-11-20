/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * Represents a failure to simulate in a scenario.
 */
public class Failure {
  private final FailureType _type;
  private final Integer _brokerId;
  private final String _rack;
  private final Long _latencyMs;

  /**
   * Constructor for broker failure.
   *
   * @param type failure type
   * @param brokerId broker ID
   */
  public Failure(FailureType type, int brokerId) {
    this(type, brokerId, null, null);
  }

  /**
   * Constructor for rack failure.
   *
   * @param type failure type (must be RACK_FAILURE)
   * @param rack rack identifier
   */
  public Failure(FailureType type, String rack) {
    this(type, null, rack, null);
  }

  /**
   * Constructor for latency-based failure.
   *
   * @param type failure type (must be SLOW_BROKER)
   * @param brokerId broker ID
   * @param latencyMs latency in milliseconds
   */
  public Failure(FailureType type, int brokerId, long latencyMs) {
    this(type, brokerId, null, latencyMs);
  }

  private Failure(FailureType type, Integer brokerId, String rack, Long latencyMs) {
    _type = type;
    _brokerId = brokerId;
    _rack = rack;
    _latencyMs = latencyMs;
  }

  public FailureType type() {
    return _type;
  }

  public Integer brokerId() {
    return _brokerId;
  }

  public String rack() {
    return _rack;
  }

  public Long latencyMs() {
    return _latencyMs;
  }

  @Override
  public String toString() {
    return String.format("Failure{type=%s, brokerId=%s, rack=%s, latencyMs=%s}",
        _type, _brokerId, _rack, _latencyMs);
  }
}
