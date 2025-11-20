/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

/**
 * Exception thrown when a user attempts to perform an operation without the required permissions.
 */
public class UnauthorizedException extends Exception {
  /**
   * Creates a new UnauthorizedException with the specified message.
   *
   * @param message the detail message
   */
  public UnauthorizedException(String message) {
    super(message);
  }

  /**
   * Creates a new UnauthorizedException with the specified message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public UnauthorizedException(String message, Throwable cause) {
    super(message, cause);
  }
}
