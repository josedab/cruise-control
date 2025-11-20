/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main orchestrator for configuration validation.
 * Coordinates multiple validators to provide comprehensive validation of Cruise Control configuration.
 *
 * Validation levels:
 * 1. Syntax - Basic property names and value formats
 * 2. Range - Numeric ranges and constraints
 * 3. Semantic - Dependencies and logical relationships
 * 4. External - Connectivity to Kafka and other systems
 */
public class ConfigurationValidator {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationValidator.class);
  private static final String CONFIG_VALIDATION_ENABLED = "config.validation.enabled";
  private static final String CONFIG_VALIDATION_FAIL_FAST = "config.validation.fail.fast";

  private final KafkaCruiseControlConfig _config;
  private final List<ConfigValidator> _validators;

  /**
   * Creates a new configuration validator.
   *
   * @param config The configuration to validate
   */
  public ConfigurationValidator(KafkaCruiseControlConfig config) {
    _config = config;
    _validators = new ArrayList<>();

    // Add validators in order of execution
    _validators.add(new SyntaxValidator());
    _validators.add(new RangeValidator());
    _validators.add(new SemanticValidator());
    _validators.add(new KafkaValidator());
  }

  /**
   * Validates the configuration using all registered validators.
   *
   * @return Aggregated validation result containing all errors and warnings
   */
  public ValidationResult validate() {
    LOG.info("Starting configuration validation with {} validators", _validators.size());

    ValidationResult aggregatedResult = new ValidationResult();
    boolean failFast = getFailFast();

    for (ConfigValidator validator : _validators) {
      try {
        LOG.debug("Running validator: {}", validator.name());
        ValidationResult result = validator.validate(_config);

        if (result.hasErrors() || result.hasWarnings()) {
          LOG.debug("Validator {} found {} errors and {} warnings",
              validator.name(), result.errorCount(), result.warningCount());
        }

        aggregatedResult.merge(result);

        // Stop on first error if fail-fast is enabled
        if (failFast && result.hasErrors()) {
          LOG.info("Stopping validation early due to fail-fast mode");
          break;
        }
      } catch (Exception e) {
        LOG.error("Validator {} threw an exception", validator.name(), e);
        aggregatedResult.addError(ValidationError.error(
            "validation.framework",
            "Validator " + validator.name() + " failed: " + e.getMessage(),
            "This is likely a bug in the validation framework. Please report it."
        ));

        if (failFast) {
          break;
        }
      }
    }

    LOG.info("Configuration validation completed: {} errors, {} warnings",
        aggregatedResult.errorCount(), aggregatedResult.warningCount());

    return aggregatedResult;
  }

  /**
   * Checks if validation is enabled in the configuration.
   *
   * @param config The configuration to check
   * @return true if validation is enabled (default), false otherwise
   */
  public static boolean isValidationEnabled(KafkaCruiseControlConfig config) {
    try {
      return config.getBoolean(CONFIG_VALIDATION_ENABLED);
    } catch (Exception e) {
      // Default to enabled if not specified
      return true;
    }
  }

  /**
   * Checks if fail-fast mode is enabled.
   *
   * @return true if validation should stop on first error
   */
  private boolean getFailFast() {
    try {
      return _config.getBoolean(CONFIG_VALIDATION_FAIL_FAST);
    } catch (Exception e) {
      // Default to false
      return false;
    }
  }
}
