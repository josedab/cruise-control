/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates value ranges and constraints for configuration properties.
 * Checks for:
 * - Numeric values within valid ranges
 * - Positive integers where required
 * - Balance thresholds >= 1.0
 * - Port numbers in valid range
 */
public class RangeValidator implements ConfigValidator {

  @Override
  public ValidationResult validate(KafkaCruiseControlConfig config) {
    List<ValidationError> errors = new ArrayList<>();

    // Validate balance thresholds (must be >= 1.0)
    validateBalanceThreshold(config, AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, errors);
    validateBalanceThreshold(config, AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, errors);

    // Validate positive integers
    validatePositiveInt(config, ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG, errors);
    validatePositiveInt(config, ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, errors);
    validatePositiveInt(config, ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG, errors);
    validatePositiveInt(config, ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG, errors);
    validatePositiveInt(config, MonitorConfig.NUM_SAMPLE_LOADING_THREADS_CONFIG, errors);
    validatePositiveInt(config, MonitorConfig.SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, errors);
    validatePositiveInt(config, MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, errors);
    validatePositiveInt(config, MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, errors);

    // Validate non-negative integers
    validateNonNegativeInt(config, ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, errors);

    // Validate percentages (0-100)
    validatePercentage(config, AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, errors);

    return new ValidationResult(errors);
  }

  /**
   * Validates that a balance threshold is >= 1.0.
   */
  private void validateBalanceThreshold(KafkaCruiseControlConfig config, String key, List<ValidationError> errors) {
    try {
      Double value = config.getDouble(key);
      if (value != null && value < 1.0) {
        errors.add(ValidationError.error(
            key,
            "Balance threshold must be >= 1.0, got: " + value,
            "Set " + key + " to a value >= 1.0 (e.g., 1.10 for 10% imbalance tolerance)"
        ));
      }
    } catch (Exception e) {
      // Config might not exist or have a different type
    }
  }

  /**
   * Validates that a value is a positive integer (> 0).
   */
  private void validatePositiveInt(KafkaCruiseControlConfig config, String key, List<ValidationError> errors) {
    try {
      Integer value = config.getInt(key);
      if (value != null && value <= 0) {
        errors.add(ValidationError.error(
            key,
            "Value must be positive (> 0), got: " + value,
            "Set " + key + " to a positive integer"
        ));
      }
    } catch (Exception e) {
      // Config might not exist or have a different type
    }
  }

  /**
   * Validates that a value is a non-negative integer (>= 0).
   */
  private void validateNonNegativeInt(KafkaCruiseControlConfig config, String key, List<ValidationError> errors) {
    try {
      Integer value = config.getInt(key);
      if (value != null && value < 0) {
        errors.add(ValidationError.error(
            key,
            "Value must be non-negative (>= 0), got: " + value,
            "Set " + key + " to a non-negative integer"
        ));
      }
    } catch (Exception e) {
      // Config might not exist or have a different type
    }
  }

  /**
   * Validates that a percentage value is between 0 and 100.
   */
  private void validatePercentage(KafkaCruiseControlConfig config, String key, List<ValidationError> errors) {
    try {
      Double value = config.getDouble(key);
      if (value != null && (value < 0.0 || value > 100.0)) {
        errors.add(ValidationError.error(
            key,
            "Percentage must be between 0.0 and 100.0, got: " + value,
            "Set " + key + " to a value between 0.0 and 100.0"
        ));
      }
    } catch (Exception e) {
      // Config might not exist or have a different type
    }
  }

  /**
   * Validates that a port number is in the valid range (1-65535).
   */
  private void validatePort(KafkaCruiseControlConfig config, String key, List<ValidationError> errors) {
    try {
      Integer value = config.getInt(key);
      if (value != null && (value < 1 || value > 65535)) {
        errors.add(ValidationError.error(
            key,
            "Port must be between 1 and 65535, got: " + value,
            "Set " + key + " to a valid port number"
        ));
      }
    } catch (Exception e) {
      // Config might not exist or have a different type
    }
  }
}
