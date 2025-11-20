/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Configuration settings for the configuration validation framework.
 * These settings control how validation is performed at startup.
 */
public final class ValidationConfig {

  private ValidationConfig() {
  }

  /**
   * <code>config.validation.enabled</code>
   */
  public static final String CONFIG_VALIDATION_ENABLED_CONFIG = "config.validation.enabled";
  public static final boolean DEFAULT_CONFIG_VALIDATION_ENABLED = true;
  public static final String CONFIG_VALIDATION_ENABLED_DOC = "Enable configuration validation on startup. "
      + "When enabled, Cruise Control will validate the configuration and fail to start if errors are found.";

  /**
   * <code>config.validation.strictness</code>
   */
  public static final String CONFIG_VALIDATION_STRICTNESS_CONFIG = "config.validation.strictness";
  public static final String DEFAULT_CONFIG_VALIDATION_STRICTNESS = "normal";
  public static final String CONFIG_VALIDATION_STRICTNESS_DOC = "Validation strictness level: "
      + "strict (warnings also fail startup), "
      + "normal (only errors fail startup), "
      + "lenient (log errors but start anyway).";

  /**
   * <code>config.validation.fail.fast</code>
   */
  public static final String CONFIG_VALIDATION_FAIL_FAST_CONFIG = "config.validation.fail.fast";
  public static final boolean DEFAULT_CONFIG_VALIDATION_FAIL_FAST = false;
  public static final String CONFIG_VALIDATION_FAIL_FAST_DOC = "If true, validation stops on the first error. "
      + "If false, all validators run and report all errors together.";

  /**
   * <code>config.validation.check.connectivity</code>
   */
  public static final String CONFIG_VALIDATION_CHECK_CONNECTIVITY_CONFIG = "config.validation.check.connectivity";
  public static final boolean DEFAULT_CONFIG_VALIDATION_CHECK_CONNECTIVITY = true;
  public static final String CONFIG_VALIDATION_CHECK_CONNECTIVITY_DOC = "Enable validation of connectivity to "
      + "external systems (Kafka, ZooKeeper). This may add a few seconds to startup time.";

  /**
   * <code>config.validation.connectivity.timeout.ms</code>
   */
  public static final String CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS_CONFIG = "config.validation.connectivity.timeout.ms";
  public static final int DEFAULT_CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS = 10000;
  public static final String CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS_DOC = "Timeout in milliseconds for "
      + "connectivity checks to external systems during validation.";

  /**
   * <code>config.validation.detailed.report</code>
   */
  public static final String CONFIG_VALIDATION_DETAILED_REPORT_CONFIG = "config.validation.detailed.report";
  public static final boolean DEFAULT_CONFIG_VALIDATION_DETAILED_REPORT = true;
  public static final String CONFIG_VALIDATION_DETAILED_REPORT_DOC = "Generate detailed validation reports with "
      + "formatting. If false, only simple error messages are logged.";

  /**
   * Defines configuration properties for validation settings.
   *
   * @param configDef The ConfigDef to add validation settings to
   * @return The updated ConfigDef
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef
        .define(CONFIG_VALIDATION_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_CONFIG_VALIDATION_ENABLED,
            ConfigDef.Importance.MEDIUM,
            CONFIG_VALIDATION_ENABLED_DOC)
        .define(CONFIG_VALIDATION_STRICTNESS_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_CONFIG_VALIDATION_STRICTNESS,
            ConfigDef.ValidString.in("strict", "normal", "lenient"),
            ConfigDef.Importance.LOW,
            CONFIG_VALIDATION_STRICTNESS_DOC)
        .define(CONFIG_VALIDATION_FAIL_FAST_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_CONFIG_VALIDATION_FAIL_FAST,
            ConfigDef.Importance.LOW,
            CONFIG_VALIDATION_FAIL_FAST_DOC)
        .define(CONFIG_VALIDATION_CHECK_CONNECTIVITY_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_CONFIG_VALIDATION_CHECK_CONNECTIVITY,
            ConfigDef.Importance.MEDIUM,
            CONFIG_VALIDATION_CHECK_CONNECTIVITY_DOC)
        .define(CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS,
            atLeast(1000),
            ConfigDef.Importance.LOW,
            CONFIG_VALIDATION_CONNECTIVITY_TIMEOUT_MS_DOC)
        .define(CONFIG_VALIDATION_DETAILED_REPORT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_CONFIG_VALIDATION_DETAILED_REPORT,
            ConfigDef.Importance.LOW,
            CONFIG_VALIDATION_DETAILED_REPORT_DOC);
  }
}
