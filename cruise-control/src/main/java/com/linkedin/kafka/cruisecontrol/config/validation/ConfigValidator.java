/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;

/**
 * Interface for configuration validators.
 * Each validator is responsible for checking a specific aspect of the configuration
 * (e.g., syntax, ranges, semantic relationships, external connectivity).
 */
public interface ConfigValidator {

  /**
   * Validates the given configuration.
   *
   * @param config The Cruise Control configuration to validate
   * @return ValidationResult containing any errors or warnings found
   */
  ValidationResult validate(KafkaCruiseControlConfig config);

  /**
   * @return A human-readable name for this validator
   */
  default String name() {
    return getClass().getSimpleName();
  }
}
