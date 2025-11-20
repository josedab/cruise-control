/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import java.util.Objects;

/**
 * Represents a configuration validation error or warning.
 * Contains the property name, severity, error message, and suggested fix.
 */
public class ValidationError {

  /**
   * Severity levels for validation errors.
   */
  public enum Severity {
    ERROR,    // Must be fixed before startup
    WARNING   // Should be reviewed but not blocking
  }

  private final String _property;
  private final Severity _severity;
  private final String _message;
  private final String _suggestion;

  /**
   * Creates a new validation error.
   *
   * @param property The configuration property that failed validation
   * @param severity The severity level (ERROR or WARNING)
   * @param message Description of the validation failure
   * @param suggestion Suggested fix for the issue
   */
  public ValidationError(String property, Severity severity, String message, String suggestion) {
    _property = Objects.requireNonNull(property, "Property cannot be null");
    _severity = Objects.requireNonNull(severity, "Severity cannot be null");
    _message = Objects.requireNonNull(message, "Message cannot be null");
    _suggestion = suggestion != null ? suggestion : "";
  }

  /**
   * Creates an error-level validation issue.
   *
   * @param property The configuration property
   * @param message Error message
   * @param suggestion Suggested fix
   * @return A new ValidationError with ERROR severity
   */
  public static ValidationError error(String property, String message, String suggestion) {
    return new ValidationError(property, Severity.ERROR, message, suggestion);
  }

  /**
   * Creates a warning-level validation issue.
   *
   * @param property The configuration property
   * @param message Warning message
   * @param suggestion Suggested fix
   * @return A new ValidationError with WARNING severity
   */
  public static ValidationError warning(String property, String message, String suggestion) {
    return new ValidationError(property, Severity.WARNING, message, suggestion);
  }

  public String getProperty() {
    return _property;
  }

  public Severity getSeverity() {
    return _severity;
  }

  public String getMessage() {
    return _message;
  }

  public String getSuggestion() {
    return _suggestion;
  }

  public boolean isError() {
    return _severity == Severity.ERROR;
  }

  public boolean isWarning() {
    return _severity == Severity.WARNING;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidationError that = (ValidationError) o;
    return Objects.equals(_property, that._property)
        && _severity == that._severity
        && Objects.equals(_message, that._message)
        && Objects.equals(_suggestion, that._suggestion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_property, _severity, _message, _suggestion);
  }

  @Override
  public String toString() {
    return String.format("[%s] %s: %s (suggestion: %s)",
        _severity, _property, _message, _suggestion);
  }
}
