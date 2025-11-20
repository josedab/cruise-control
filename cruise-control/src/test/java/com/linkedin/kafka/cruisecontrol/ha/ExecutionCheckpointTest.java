/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ExecutionCheckpoint.
 */
public class ExecutionCheckpointTest {

  @Test
  public void testCheckpointSerialization() {
    // Create a checkpoint
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("pendingTasks", 5);
    metadata.put("completedTasks", 10);

    ExecutionCheckpoint original = new ExecutionCheckpoint(
        "test-uuid-123",
        System.currentTimeMillis(),
        "EXECUTION_STARTED",
        true,
        false,
        "test rebalance",
        metadata
    );

    // Serialize to JSON
    byte[] json = original.toJson();
    assertNotNull(json);
    assertTrue(json.length > 0);

    // Deserialize back
    ExecutionCheckpoint deserialized = ExecutionCheckpoint.fromJson(json);

    // Verify fields match
    assertEquals(original.uuid(), deserialized.uuid());
    assertEquals(original.checkpointTimeMs(), deserialized.checkpointTimeMs());
    assertEquals(original.executorState(), deserialized.executorState());
    assertEquals(original.hasOngoingExecution(), deserialized.hasOngoingExecution());
    assertEquals(original.isKafkaAssignerMode(), deserialized.isKafkaAssignerMode());
    assertEquals(original.reason(), deserialized.reason());
    assertEquals(original.executionMetadata(), deserialized.executionMetadata());
  }

  @Test
  public void testCheckpointEquality() {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("key", "value");

    ExecutionCheckpoint cp1 = new ExecutionCheckpoint(
        "uuid1", 1000L, "STATE1", true, false, "reason1", metadata
    );

    ExecutionCheckpoint cp2 = new ExecutionCheckpoint(
        "uuid1", 1000L, "STATE1", true, false, "reason1", metadata
    );

    assertEquals(cp1, cp2);
    assertEquals(cp1.hashCode(), cp2.hashCode());
  }

  @Test
  public void testCheckpointInequality() {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("key", "value");

    ExecutionCheckpoint cp1 = new ExecutionCheckpoint(
        "uuid1", 1000L, "STATE1", true, false, "reason1", metadata
    );

    ExecutionCheckpoint cp2 = new ExecutionCheckpoint(
        "uuid2", 1000L, "STATE1", true, false, "reason1", metadata
    );

    assertNotEquals(cp1, cp2);
  }

  @Test
  public void testToString() {
    Map<String, Object> metadata = new HashMap<>();
    ExecutionCheckpoint checkpoint = new ExecutionCheckpoint(
        "test-uuid", 123456L, "STARTED", true, false, "test", metadata
    );

    String str = checkpoint.toString();
    assertNotNull(str);
    assertTrue(str.contains("test-uuid"));
    assertTrue(str.contains("STARTED"));
  }
}
