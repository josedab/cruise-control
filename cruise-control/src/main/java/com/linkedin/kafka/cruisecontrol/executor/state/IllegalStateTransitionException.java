/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.state;

/**
 * Exception thrown when an invalid state transition is attempted.
 */
public class IllegalStateTransitionException extends RuntimeException {

  /**
   * Creates a new exception with the given message.
   *
   * @param message the error message
   */
  public IllegalStateTransitionException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with the given message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public IllegalStateTransitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
