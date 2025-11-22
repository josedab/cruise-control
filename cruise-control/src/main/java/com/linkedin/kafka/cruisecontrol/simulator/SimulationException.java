/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

/**
 * Exception thrown when a simulation operation fails.
 */
public class SimulationException extends Exception {

  /**
   * Constructor with message.
   *
   * @param message error message
   */
  public SimulationException(String message) {
    super(message);
  }

  /**
   * Constructor with message and cause.
   *
   * @param message error message
   * @param cause underlying cause
   */
  public SimulationException(String message, Throwable cause) {
    super(message, cause);
  }
}
