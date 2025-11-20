/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.kafka;

/**
 * Exception thrown when Kafka execution operations fail.
 */
public class KafkaExecutionException extends RuntimeException {

  /**
   * Creates a new exception with the given message.
   *
   * @param message the error message
   */
  public KafkaExecutionException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with the given message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public KafkaExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
