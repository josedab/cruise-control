/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates semantic relationships between configuration properties.
 * Checks for:
 * - Dependencies between related configurations
 * - Conflicting settings
 * - Logical consistency
 */
public class SemanticValidator implements ConfigValidator {

  private static final String SELF_HEALING_ENABLED_CONFIG = "self.healing.enabled";
  private static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = "anomaly.detection.allow.capacity.estimation";

  @Override
  public ValidationResult validate(KafkaCruiseControlConfig config) {
    List<ValidationError> errors = new ArrayList<>();

    // Validate anomaly detection dependencies
    validateAnomalyDetectionDependencies(config, errors);

    // Validate throttling configuration consistency
    validateThrottlingConsistency(config, errors);

    // Validate goal ordering best practices
    validateGoalOrdering(config, errors);

    return new ValidationResult(errors);
  }

  /**
   * Validates that anomaly detection has proper action configured (notifier or self-healing).
   */
  private void validateAnomalyDetectionDependencies(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      // Check if anomaly detection is enabled
      boolean detectionEnabled = false;
      try {
        detectionEnabled = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
      } catch (Exception e) {
        // Config might not exist, default to false
      }

      // Check if anomaly detection goals are configured
      List<String> anomalyGoals = config.getList(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG);
      boolean hasGoals = anomalyGoals != null && !anomalyGoals.isEmpty();

      if (detectionEnabled || hasGoals) {
        // Must have notifier or self-healing enabled
        boolean hasSelfHealing = false;
        try {
          hasSelfHealing = config.getBoolean(SELF_HEALING_ENABLED_CONFIG);
        } catch (Exception e) {
          // Config might not exist
        }

        String notifierClass = config.getString(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG);
        boolean hasNotifier = notifierClass != null
            && !notifierClass.isEmpty()
            && !notifierClass.contains("NoopNotifier");

        if (!hasSelfHealing && !hasNotifier) {
          errors.add(ValidationError.warning(
              AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
              "Anomaly detection is configured but no action is enabled",
              "Enable self-healing (" + SELF_HEALING_ENABLED_CONFIG + "=true) OR " +
              "configure a notifier (" + AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG + "=<notifier-class>)"
          ));
        }
      }
    } catch (Exception e) {
      // Gracefully handle any unexpected errors
    }
  }

  /**
   * Validates that throttling configuration is consistent.
   */
  private void validateThrottlingConsistency(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      Integer clusterMax = config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
      Integer brokerMax = config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);

      if (clusterMax != null && brokerMax != null) {
        // Simple heuristic: If broker limit × 10 < cluster limit, likely misconfigured
        // This is a warning because it might be intentional in some setups
        if (brokerMax * 10 < clusterMax) {
          errors.add(ValidationError.warning(
              ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG,
              String.format(
                  "Cluster max (%d) may be too high for broker max (%d). " +
                  "With %d concurrent movements per broker, you may not reach the cluster limit efficiently.",
                  clusterMax, brokerMax, brokerMax
              ),
              "Consider reducing " + ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG +
              " or increasing " + ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG
          ));
        }

        // Check if cluster max is less than broker max (definitely wrong)
        if (clusterMax < brokerMax) {
          errors.add(ValidationError.error(
              ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG,
              String.format(
                  "Cluster max (%d) is less than broker max (%d). " +
                  "This will severely limit execution performance.",
                  clusterMax, brokerMax
              ),
              "Increase " + ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG +
              " to be at least equal to " + ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG
          ));
        }
      }

      // Validate leader movements vs partition movements
      Integer leaderMax = config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
      if (clusterMax != null && leaderMax != null && leaderMax > clusterMax * 2) {
        errors.add(ValidationError.warning(
            ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
            String.format(
                "Leader movements limit (%d) is much higher than cluster movements (%d). " +
                "This may cause imbalanced execution.",
                leaderMax, clusterMax
            ),
            "Consider adjusting the ratio between leader and partition movements"
        ));
      }
    } catch (Exception e) {
      // Gracefully handle any unexpected errors
    }
  }

  /**
   * Validates goal ordering follows best practices.
   */
  private void validateGoalOrdering(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      List<String> goals = config.getList("goals");
      if (goals == null || goals.isEmpty()) {
        return;
      }

      // Check if RackAwareGoal should be first
      boolean hasRackAwareGoal = goals.stream()
          .anyMatch(g -> g.contains("RackAwareGoal") || g.endsWith(".RackAwareGoal"));

      if (hasRackAwareGoal) {
        int rackAwareIndex = -1;
        for (int i = 0; i < goals.size(); i++) {
          if (goals.get(i).contains("RackAwareGoal") || goals.get(i).endsWith(".RackAwareGoal")) {
            rackAwareIndex = i;
            break;
          }
        }

        if (rackAwareIndex > 0) {
          errors.add(ValidationError.warning(
              "goals",
              "RackAwareGoal is not the first goal (currently at position " + rackAwareIndex + ")",
              "Consider moving RackAwareGoal to the first position for better rack distribution. " +
              "Example: goals=RackAwareGoal,..."
          ));
        }
      }

      // Check if capacity goals come before distribution goals
      int lastCapacityIndex = -1;
      int firstDistributionIndex = -1;

      for (int i = 0; i < goals.size(); i++) {
        String goal = goals.get(i);
        if (goal.contains("Capacity")) {
          lastCapacityIndex = i;
        }
        if (goal.contains("Distribution") && firstDistributionIndex == -1) {
          firstDistributionIndex = i;
        }
      }

      if (lastCapacityIndex > 0 && firstDistributionIndex > 0 && lastCapacityIndex > firstDistributionIndex) {
        errors.add(ValidationError.warning(
            "goals",
            "Some capacity goals are ordered after distribution goals",
            "Best practice: order capacity goals before distribution goals. " +
            "Capacity goals ensure hard constraints are met, while distribution goals optimize balance."
        ));
      }
    } catch (Exception e) {
      // Gracefully handle any unexpected errors
    }
  }
}
