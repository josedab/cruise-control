/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a checkpoint of execution state for high availability failover.
 *
 * <p>This checkpoint contains sufficient state information to allow a standby
 * Cruise Control instance to resume execution after a leader failure.
 *
 * <p>The checkpoint is serialized to JSON and stored in a Kafka topic for
 * durability and replication.
 */
public class ExecutionCheckpoint {
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private final String _uuid;
  private final long _checkpointTimeMs;
  private final String _executorState;
  private final boolean _hasOngoingExecution;
  private final boolean _isKafkaAssignerMode;
  private final String _reason;
  private final Map<String, Object> _executionMetadata;

  /**
   * Create an execution checkpoint.
   *
   * @param uuid unique identifier for the execution
   * @param checkpointTimeMs timestamp when checkpoint was created
   * @param executorState current state of the executor (e.g., "EXECUTION_STARTED")
   * @param hasOngoingExecution whether there is an ongoing execution
   * @param isKafkaAssignerMode whether in Kafka assigner mode
   * @param reason reason for the execution
   * @param executionMetadata additional execution metadata (task counts, progress, etc.)
   */
  public ExecutionCheckpoint(String uuid,
                            long checkpointTimeMs,
                            String executorState,
                            boolean hasOngoingExecution,
                            boolean isKafkaAssignerMode,
                            String reason,
                            Map<String, Object> executionMetadata) {
    this._uuid = uuid;
    this._checkpointTimeMs = checkpointTimeMs;
    this._executorState = executorState;
    this._hasOngoingExecution = hasOngoingExecution;
    this._isKafkaAssignerMode = isKafkaAssignerMode;
    this._reason = reason;
    this._executionMetadata = executionMetadata;
  }

  public String uuid() {
    return _uuid;
  }

  public long checkpointTimeMs() {
    return _checkpointTimeMs;
  }

  public String executorState() {
    return _executorState;
  }

  public boolean hasOngoingExecution() {
    return _hasOngoingExecution;
  }

  public boolean isKafkaAssignerMode() {
    return _isKafkaAssignerMode;
  }

  public String reason() {
    return _reason;
  }

  public Map<String, Object> executionMetadata() {
    return _executionMetadata;
  }

  /**
   * Serialize this checkpoint to JSON bytes.
   *
   * @return JSON representation as bytes
   */
  public byte[] toJson() {
    String json = GSON.toJson(this);
    return json.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Deserialize a checkpoint from JSON bytes.
   *
   * @param jsonBytes JSON representation
   * @return the deserialized checkpoint
   */
  public static ExecutionCheckpoint fromJson(byte[] jsonBytes) {
    String json = new String(jsonBytes, StandardCharsets.UTF_8);
    return GSON.fromJson(json, ExecutionCheckpoint.class);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutionCheckpoint that = (ExecutionCheckpoint) o;
    return _checkpointTimeMs == that._checkpointTimeMs
        && _hasOngoingExecution == that._hasOngoingExecution
        && _isKafkaAssignerMode == that._isKafkaAssignerMode
        && Objects.equals(_uuid, that._uuid)
        && Objects.equals(_executorState, that._executorState)
        && Objects.equals(_reason, that._reason)
        && Objects.equals(_executionMetadata, that._executionMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_uuid, _checkpointTimeMs, _executorState, _hasOngoingExecution,
                       _isKafkaAssignerMode, _reason, _executionMetadata);
  }

  @Override
  public String toString() {
    return String.format("ExecutionCheckpoint{uuid=%s, checkpointTime=%d, state=%s, hasOngoing=%s}",
                        _uuid, _checkpointTimeMs, _executorState, _hasOngoingExecution);
  }
}
