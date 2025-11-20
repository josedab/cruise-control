/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

/**
 * A class to keep Cruise Control OpenTelemetry Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class OpenTelemetryConfig {

  /**
   * <code>opentelemetry.enabled</code>
   */
  public static final String OPENTELEMETRY_ENABLED_CONFIG = "opentelemetry.enabled";
  public static final boolean DEFAULT_OPENTELEMETRY_ENABLED = false;
  public static final String OPENTELEMETRY_ENABLED_DOC = "Enable OpenTelemetry distributed tracing. "
      + "When enabled, Cruise Control will instrument key operations with OpenTelemetry spans for observability.";

  /**
   * <code>opentelemetry.exporter.otlp.endpoint</code>
   */
  public static final String OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG = "opentelemetry.exporter.otlp.endpoint";
  public static final String DEFAULT_OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317";
  public static final String OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT_DOC = "The OTLP endpoint to export traces to. "
      + "This should be the gRPC endpoint of your observability backend (e.g., Jaeger, Zipkin, or other OTLP-compatible collector). "
      + "Example: http://jaeger:4317";

  /**
   * <code>opentelemetry.service.name</code>
   */
  public static final String OPENTELEMETRY_SERVICE_NAME_CONFIG = "opentelemetry.service.name";
  public static final String DEFAULT_OPENTELEMETRY_SERVICE_NAME = "cruise-control";
  public static final String OPENTELEMETRY_SERVICE_NAME_DOC = "The service name to use for OpenTelemetry traces. "
      + "This will be used to identify the Cruise Control service in your observability backend.";

  private OpenTelemetryConfig() {
  }

  /**
   * Define the OpenTelemetry configs.
   * @param configDef Config definition.
   * @return the updated ConfigDef.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(OPENTELEMETRY_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_OPENTELEMETRY_ENABLED,
                            ConfigDef.Importance.LOW,
                            OPENTELEMETRY_ENABLED_DOC)
                    .define(OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT,
                            ConfigDef.Importance.LOW,
                            OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT_DOC)
                    .define(OPENTELEMETRY_SERVICE_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_OPENTELEMETRY_SERVICE_NAME,
                            ConfigDef.Importance.LOW,
                            OPENTELEMETRY_SERVICE_NAME_DOC);
  }
}
