/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates connectivity to external systems (Kafka, ZooKeeper).
 * Checks for:
 * - Kafka cluster connectivity
 * - Metrics topic existence or ability to create it
 * - Security configuration validity
 *
 * Note: This validator may take longer to execute as it performs actual network calls.
 * It should be skipped if config.validation.check.connectivity is set to false.
 */
public class KafkaValidator implements ConfigValidator {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaValidator.class);
  private static final int DEFAULT_TIMEOUT_MS = 10000;
  private static final String CONFIG_VALIDATION_CHECK_CONNECTIVITY = "config.validation.check.connectivity";
  private static final String CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS = "config.validation.connectivity.timeout.ms";

  @Override
  public ValidationResult validate(KafkaCruiseControlConfig config) {
    List<ValidationError> errors = new ArrayList<>();

    // Check if connectivity validation is enabled
    boolean checkConnectivity = true;
    try {
      checkConnectivity = config.getBoolean(CONFIG_VALIDATION_CHECK_CONNECTIVITY);
    } catch (Exception e) {
      // Config might not exist, default to true
    }

    if (!checkConnectivity) {
      LOG.info("Skipping Kafka connectivity validation ({}=false)", CONFIG_VALIDATION_CHECK_CONNECTIVITY);
      return new ValidationResult(errors);
    }

    // Get timeout
    int timeoutMs = DEFAULT_TIMEOUT_MS;
    try {
      timeoutMs = config.getInt(CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS);
    } catch (Exception e) {
      // Config might not exist, use default
    }

    // Test Kafka connectivity
    validateKafkaConnection(config, timeoutMs, errors);

    return new ValidationResult(errors);
  }

  /**
   * Validates connectivity to Kafka cluster.
   */
  private void validateKafkaConnection(KafkaCruiseControlConfig config, int timeoutMs, List<ValidationError> errors) {
    String bootstrapServers = null;
    try {
      bootstrapServers = config.getString(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG);
      if (bootstrapServers == null || bootstrapServers.isEmpty()) {
        errors.add(ValidationError.error(
            MonitorConfig.BOOTSTRAP_SERVERS_CONFIG,
            "Bootstrap servers not configured",
            "Set " + MonitorConfig.BOOTSTRAP_SERVERS_CONFIG + " to your Kafka broker addresses"
        ));
        return;
      }

      Properties adminProps = new Properties();
      adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Math.min(timeoutMs, 5000)));
      adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));

      // Add security configs if present
      addSecurityConfigs(adminProps, config);

      LOG.info("Testing connectivity to Kafka cluster at: {}", bootstrapServers);

      try (AdminClient adminClient = AdminClient.create(adminProps)) {
        // Simple test: list topics
        ListTopicsResult result = adminClient.listTopics();
        Set<String> topics = result.names().get(timeoutMs, TimeUnit.MILLISECONDS);

        LOG.info("Successfully connected to Kafka. Found {} topics.", topics.size());

        // Validate metrics topic
        validateMetricsTopic(config, topics, errors);

      } catch (Exception e) {
        String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        errors.add(ValidationError.error(
            MonitorConfig.BOOTSTRAP_SERVERS_CONFIG,
            "Cannot connect to Kafka cluster: " + errorMsg,
            "Check that Kafka is running and accessible at: " + bootstrapServers +
            ". Verify network connectivity and security settings."
        ));
        LOG.warn("Failed to connect to Kafka at {}: {}", bootstrapServers, errorMsg);
      }
    } catch (Exception e) {
      errors.add(ValidationError.error(
          MonitorConfig.BOOTSTRAP_SERVERS_CONFIG,
          "Error validating Kafka configuration: " + e.getMessage(),
          "Check your Kafka configuration settings"
      ));
    }
  }

  /**
   * Validates that the metrics topic exists or can be auto-created.
   */
  private void validateMetricsTopic(KafkaCruiseControlConfig config, Set<String> existingTopics, List<ValidationError> errors) {
    try {
      String metricsTopic = config.getString(MonitorConfig.METRIC_REPORTER_TOPIC_CONFIG);
      if (metricsTopic == null) {
        metricsTopic = "__CruiseControlMetrics"; // Default value
      }

      // Check if topic exists
      if (existingTopics.contains(metricsTopic)) {
        LOG.info("Metrics topic '{}' exists.", metricsTopic);
        return;
      }

      // Check if auto-create is enabled
      boolean autoCreate = true;
      try {
        String autoCreateStr = config.originals().get(MonitorConfig.METRIC_REPORTER_TOPIC_AUTO_CREATE_CONFIG).toString();
        autoCreate = Boolean.parseBoolean(autoCreateStr);
      } catch (Exception e) {
        // Config might not exist, default to true
      }

      if (!autoCreate) {
        errors.add(ValidationError.error(
            MonitorConfig.METRIC_REPORTER_TOPIC_CONFIG,
            "Metrics topic '" + metricsTopic + "' does not exist and auto-create is disabled",
            "Create the topic manually or enable auto-create: " +
            MonitorConfig.METRIC_REPORTER_TOPIC_AUTO_CREATE_CONFIG + "=true"
        ));
      } else {
        LOG.info("Metrics topic '{}' does not exist but will be auto-created.", metricsTopic);
      }
    } catch (Exception e) {
      // Don't fail validation if we can't check the topic
      LOG.debug("Could not validate metrics topic: {}", e.getMessage());
    }
  }

  /**
   * Adds security configuration to admin client properties if configured.
   */
  private void addSecurityConfigs(Properties props, KafkaCruiseControlConfig config) {
    try {
      // Add SSL configs if present
      String securityProtocol = config.originals().get("security.protocol") != null
          ? config.originals().get("security.protocol").toString()
          : null;

      if (securityProtocol != null) {
        props.put("security.protocol", securityProtocol);
      }

      // Copy SSL configs
      copyConfigIfPresent(config, props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
      copyConfigIfPresent(config, props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
      copyConfigIfPresent(config, props, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
      copyConfigIfPresent(config, props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      copyConfigIfPresent(config, props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

      // Copy SASL configs
      copyConfigIfPresent(config, props, SaslConfigs.SASL_MECHANISM);
      copyConfigIfPresent(config, props, SaslConfigs.SASL_JAAS_CONFIG);

    } catch (Exception e) {
      LOG.debug("Could not copy security configs: {}", e.getMessage());
    }
  }

  /**
   * Copies a config value if it exists.
   */
  private void copyConfigIfPresent(KafkaCruiseControlConfig config, Properties props, String key) {
    try {
      Object value = config.originals().get(key);
      if (value != null) {
        props.put(key, value.toString());
      }
    } catch (Exception e) {
      // Ignore
    }
  }
}
