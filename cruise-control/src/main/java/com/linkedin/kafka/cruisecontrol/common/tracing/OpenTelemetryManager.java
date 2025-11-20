/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common.tracing;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.OpenTelemetryConfig;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages OpenTelemetry initialization and provides access to tracers for distributed tracing.
 * This class is responsible for configuring the OpenTelemetry SDK based on Cruise Control configuration
 * and providing tracer instances for instrumentation.
 */
public class OpenTelemetryManager {
  private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryManager.class);
  private static final String INSTRUMENTATION_NAME = "kafka-cruise-control";
  private static final String INSTRUMENTATION_VERSION = "1.0.0";

  private final boolean _enabled;
  private final Tracer _tracer;
  private final OpenTelemetry _openTelemetry;

  /**
   * Creates an OpenTelemetryManager instance.
   *
   * @param config The Cruise Control configuration
   */
  public OpenTelemetryManager(KafkaCruiseControlConfig config) {
    _enabled = config.getBoolean(OpenTelemetryConfig.OPENTELEMETRY_ENABLED_CONFIG);

    if (_enabled) {
      String serviceName = config.getString(OpenTelemetryConfig.OPENTELEMETRY_SERVICE_NAME_CONFIG);
      String otlpEndpoint = config.getString(OpenTelemetryConfig.OPENTELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG);

      LOG.info("Initializing OpenTelemetry with service name: {} and OTLP endpoint: {}", serviceName, otlpEndpoint);

      // Create resource with service name
      Resource resource = Resource.getDefault()
          .merge(Resource.builder()
              .put(ResourceAttributes.SERVICE_NAME, serviceName)
              .build());

      // Configure OTLP exporter
      OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
          .setEndpoint(otlpEndpoint)
          .build();

      // Create tracer provider with batch span processor
      SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
          .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
          .setResource(resource)
          .build();

      // Build OpenTelemetry SDK
      _openTelemetry = OpenTelemetrySdk.builder()
          .setTracerProvider(tracerProvider)
          .buildAndRegisterGlobal();

      _tracer = _openTelemetry.getTracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION);

      LOG.info("OpenTelemetry initialized successfully");
    } else {
      LOG.info("OpenTelemetry is disabled");
      _openTelemetry = GlobalOpenTelemetry.get();
      _tracer = _openTelemetry.getTracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION);
    }
  }

  /**
   * Returns whether OpenTelemetry tracing is enabled.
   *
   * @return true if tracing is enabled, false otherwise
   */
  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Returns the tracer instance for creating spans.
   *
   * @return the OpenTelemetry tracer
   */
  public Tracer getTracer() {
    return _tracer;
  }

  /**
   * Returns the OpenTelemetry instance.
   *
   * @return the OpenTelemetry instance
   */
  public OpenTelemetry getOpenTelemetry() {
    return _openTelemetry;
  }

  /**
   * Shuts down the OpenTelemetry SDK and flushes any pending spans.
   */
  public void shutdown() {
    if (_enabled && _openTelemetry instanceof OpenTelemetrySdk) {
      LOG.info("Shutting down OpenTelemetry");
      ((OpenTelemetrySdk) _openTelemetry).getSdkTracerProvider().shutdown();
    }
  }
}
