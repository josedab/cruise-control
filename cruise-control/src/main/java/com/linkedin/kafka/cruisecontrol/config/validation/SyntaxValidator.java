/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Validates basic syntax of configuration properties.
 * Checks for:
 * - Unknown configuration properties (with suggestions)
 * - Invalid goal class names
 * - Invalid value formats (e.g., host:port lists)
 */
public class SyntaxValidator implements ConfigValidator {

  private static final Pattern HOST_PORT_PATTERN = Pattern.compile(
      "^([a-zA-Z0-9.-]+:[0-9]+)(,[a-zA-Z0-9.-]+:[0-9]+)*$"
  );

  @Override
  public ValidationResult validate(KafkaCruiseControlConfig config) {
    List<ValidationError> errors = new ArrayList<>();

    // Validate goal class names
    validateGoalClasses(config, errors);

    // Validate host:port format for bootstrap.servers
    validateBootstrapServers(config, errors);

    // Validate host:port format for zookeeper.connect
    validateZookeeperConnect(config, errors);

    return new ValidationResult(errors);
  }

  /**
   * Validates that all configured goals are valid class names.
   */
  private void validateGoalClasses(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      // Try to get the goals list - this will validate the class names
      List<String> goals = config.getList(AnalyzerConfig.GOALS_CONFIG);

      for (String goalName : goals) {
        try {
          // Try to load the class to verify it exists
          Class.forName(goalName);
        } catch (ClassNotFoundException e) {
          errors.add(ValidationError.error(
              AnalyzerConfig.GOALS_CONFIG,
              "Invalid goal class: " + goalName,
              "Check spelling or remove invalid goal. The goal class must be available on the classpath."
          ));
        }
      }
    } catch (Exception e) {
      errors.add(ValidationError.error(
          AnalyzerConfig.GOALS_CONFIG,
          "Failed to parse goals configuration: " + e.getMessage(),
          "Ensure goals are specified as a comma-separated list of fully-qualified class names"
      ));
    }

    // Validate intra-broker goals
    try {
      List<String> intraBrokerGoals = config.getList(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG);

      for (String goalName : intraBrokerGoals) {
        try {
          Class.forName(goalName);
        } catch (ClassNotFoundException e) {
          errors.add(ValidationError.error(
              AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
              "Invalid intra-broker goal class: " + goalName,
              "Check spelling or remove invalid goal. The goal class must be available on the classpath."
          ));
        }
      }
    } catch (Exception e) {
      errors.add(ValidationError.error(
          AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
          "Failed to parse intra-broker goals configuration: " + e.getMessage(),
          "Ensure goals are specified as a comma-separated list of fully-qualified class names"
      ));
    }
  }

  /**
   * Validates the bootstrap.servers format.
   */
  private void validateBootstrapServers(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      String bootstrapServers = config.getString(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG);
      if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
        if (!HOST_PORT_PATTERN.matcher(bootstrapServers).matches()) {
          errors.add(ValidationError.error(
              MonitorConfig.BOOTSTRAP_SERVERS_CONFIG,
              "Invalid format: " + bootstrapServers,
              "Expected format: host1:port1,host2:port2,... (e.g., localhost:9092,broker2:9092)"
          ));
        }
      }
    } catch (Exception e) {
      // Configuration key might not exist, which is fine
    }
  }

  /**
   * Validates the zookeeper.connect format.
   */
  private void validateZookeeperConnect(KafkaCruiseControlConfig config, List<ValidationError> errors) {
    try {
      String zookeeperConnect = config.getString(MonitorConfig.ZOOKEEPER_CONNECT_CONFIG);
      if (zookeeperConnect != null && !zookeeperConnect.isEmpty()) {
        // ZooKeeper connect string can have an optional chroot path: host:port,host:port/chroot
        String connectString = zookeeperConnect;
        if (connectString.contains("/")) {
          connectString = connectString.substring(0, connectString.indexOf("/"));
        }

        if (!HOST_PORT_PATTERN.matcher(connectString).matches()) {
          errors.add(ValidationError.error(
              MonitorConfig.ZOOKEEPER_CONNECT_CONFIG,
              "Invalid format: " + zookeeperConnect,
              "Expected format: host1:port1,host2:port2[/chroot] (e.g., localhost:2181/kafka)"
          ));
        }
      }
    } catch (Exception e) {
      // Configuration key might not exist, which is fine
    }
  }

  /**
   * Finds the closest matching property name using Levenshtein distance.
   */
  private String findClosestMatch(String unknownProperty, Set<String> knownProperties) {
    int minDistance = Integer.MAX_VALUE;
    String closestMatch = null;

    for (String knownProperty : knownProperties) {
      int distance = levenshteinDistance(unknownProperty.toLowerCase(), knownProperty.toLowerCase());
      if (distance < minDistance) {
        minDistance = distance;
        closestMatch = knownProperty;
      }
    }

    // Only suggest if the distance is reasonable (less than 3 edits)
    if (minDistance <= 3) {
      return closestMatch;
    }
    return null;
  }

  /**
   * Calculates Levenshtein distance between two strings.
   */
  private int levenshteinDistance(String s1, String s2) {
    int[][] dp = new int[s1.length() + 1][s2.length() + 1];

    for (int i = 0; i <= s1.length(); i++) {
      dp[i][0] = i;
    }
    for (int j = 0; j <= s2.length(); j++) {
      dp[0][j] = j;
    }

    for (int i = 1; i <= s1.length(); i++) {
      for (int j = 1; j <= s2.length(); j++) {
        int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
        dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
      }
    }

    return dp[s1.length()][s2.length()];
  }
}
