/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.audit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single audit log entry capturing details about an operation performed in Cruise Control.
 */
public class AuditLogEntry {
  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();

  private final Instant _timestamp;
  private final String _requestId;
  private final String _user;
  private final String _sourceIP;
  private final String _operation;
  private final String _resource;
  private final Map<String, String> _parameters;
  private final String _result;
  private final String _errorMessage;
  private final long _durationMs;
  private final Map<String, Object> _metadata;

  private AuditLogEntry(Builder builder) {
    _timestamp = builder._timestamp;
    _requestId = builder._requestId;
    _user = builder._user;
    _sourceIP = builder._sourceIP;
    _operation = builder._operation;
    _resource = builder._resource;
    _parameters = Collections.unmodifiableMap(new HashMap<>(builder._parameters));
    _result = builder._result;
    _errorMessage = builder._errorMessage;
    _durationMs = builder._durationMs;
    _metadata = Collections.unmodifiableMap(new HashMap<>(builder._metadata));
  }

  public Instant timestamp() {
    return _timestamp;
  }

  public String requestId() {
    return _requestId;
  }

  public String user() {
    return _user;
  }

  public String sourceIP() {
    return _sourceIP;
  }

  public String operation() {
    return _operation;
  }

  public String resource() {
    return _resource;
  }

  public Map<String, String> parameters() {
    return _parameters;
  }

  public String result() {
    return _result;
  }

  public String errorMessage() {
    return _errorMessage;
  }

  public long durationMs() {
    return _durationMs;
  }

  public Map<String, Object> metadata() {
    return _metadata;
  }

  /**
   * Converts this audit log entry to a JSON string.
   *
   * @return JSON representation of this entry
   */
  public String toJson() {
    return GSON.toJson(this);
  }

  /**
   * Converts this audit log entry to a compact JSON string (single line).
   *
   * @return compact JSON representation
   */
  public String toCompactJson() {
    return new Gson().toJson(this);
  }

  @Override
  public String toString() {
    return toCompactJson();
  }

  /**
   * Creates a new builder for constructing audit log entries.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating AuditLogEntry instances.
   */
  public static class Builder {
    private Instant _timestamp = Instant.now();
    private String _requestId;
    private String _user;
    private String _sourceIP;
    private String _operation;
    private String _resource;
    private Map<String, String> _parameters = new HashMap<>();
    private String _result;
    private String _errorMessage;
    private long _durationMs;
    private Map<String, Object> _metadata = new HashMap<>();

    public Builder timestamp(Instant timestamp) {
      _timestamp = timestamp;
      return this;
    }

    public Builder requestId(String requestId) {
      _requestId = requestId;
      return this;
    }

    public Builder user(String user) {
      _user = user;
      return this;
    }

    public Builder sourceIP(String sourceIP) {
      _sourceIP = sourceIP;
      return this;
    }

    public Builder operation(String operation) {
      _operation = operation;
      return this;
    }

    public Builder resource(String resource) {
      _resource = resource;
      return this;
    }

    public Builder parameters(Map<String, String> parameters) {
      if (parameters != null) {
        _parameters.putAll(parameters);
      }
      return this;
    }

    public Builder parameter(String key, String value) {
      _parameters.put(key, value);
      return this;
    }

    public Builder result(String result) {
      _result = result;
      return this;
    }

    public Builder errorMessage(String errorMessage) {
      _errorMessage = errorMessage;
      return this;
    }

    public Builder durationMs(long durationMs) {
      _durationMs = durationMs;
      return this;
    }

    public Builder metadata(Map<String, Object> metadata) {
      if (metadata != null) {
        _metadata.putAll(metadata);
      }
      return this;
    }

    public Builder metadata(String key, Object value) {
      _metadata.put(key, value);
      return this;
    }

    public AuditLogEntry build() {
      Objects.requireNonNull(_requestId, "Request ID cannot be null");
      Objects.requireNonNull(_user, "User cannot be null");
      Objects.requireNonNull(_operation, "Operation cannot be null");
      Objects.requireNonNull(_result, "Result cannot be null");
      return new AuditLogEntry(this);
    }
  }
}
