/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains the results of configuration validation.
 * Aggregates all errors and warnings found during validation.
 */
public class ValidationResult {

  private final List<ValidationError> _errors;

  /**
   * Creates a validation result with the given errors.
   *
   * @param errors List of validation errors (can include warnings)
   */
  public ValidationResult(List<ValidationError> errors) {
    _errors = new ArrayList<>(errors);
  }

  /**
   * Creates an empty validation result (no errors or warnings).
   */
  public ValidationResult() {
    _errors = new ArrayList<>();
  }

  /**
   * @return All validation errors and warnings
   */
  public List<ValidationError> getAll() {
    return Collections.unmodifiableList(_errors);
  }

  /**
   * @return Only ERROR-level validation issues
   */
  public List<ValidationError> getErrors() {
    return _errors.stream()
        .filter(ValidationError::isError)
        .collect(Collectors.toList());
  }

  /**
   * @return Only WARNING-level validation issues
   */
  public List<ValidationError> getWarnings() {
    return _errors.stream()
        .filter(ValidationError::isWarning)
        .collect(Collectors.toList());
  }

  /**
   * @return true if any errors are present
   */
  public boolean hasErrors() {
    return _errors.stream().anyMatch(ValidationError::isError);
  }

  /**
   * @return true if any warnings are present
   */
  public boolean hasWarnings() {
    return _errors.stream().anyMatch(ValidationError::isWarning);
  }

  /**
   * @return true if validation passed with no errors or warnings
   */
  public boolean isValid() {
    return _errors.isEmpty();
  }

  /**
   * @return Number of errors
   */
  public int errorCount() {
    return (int) _errors.stream().filter(ValidationError::isError).count();
  }

  /**
   * @return Number of warnings
   */
  public int warningCount() {
    return (int) _errors.stream().filter(ValidationError::isWarning).count();
  }

  /**
   * Merges another validation result into this one.
   *
   * @param other The other validation result to merge
   */
  public void merge(ValidationResult other) {
    _errors.addAll(other._errors);
  }

  /**
   * Adds a single validation error.
   *
   * @param error The validation error to add
   */
  public void addError(ValidationError error) {
    _errors.add(error);
  }

  @Override
  public String toString() {
    if (isValid()) {
      return "Validation passed (0 errors, 0 warnings)";
    }
    return String.format("Validation result: %d errors, %d warnings",
        errorCount(), warningCount());
  }
}
