/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.audit;

import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AuditLogEntryTest {

  @Test
  public void testAuditLogEntryCreation() {
    Instant now = Instant.now();
    Map<String, String> params = new HashMap<>();
    params.put("cluster", "us-east");
    params.put("goals", "RackAwareGoal");

    AuditLogEntry entry = AuditLogEntry.builder()
        .timestamp(now)
        .requestId("req-123")
        .user("alice@example.com")
        .sourceIP("10.0.1.50")
        .operation("REBALANCE")
        .resource("cluster:us-east")
        .parameters(params)
        .result("SUCCESS")
        .durationMs(1800000)
        .build();

    assertEquals(now, entry.timestamp());
    assertEquals("req-123", entry.requestId());
    assertEquals("alice@example.com", entry.user());
    assertEquals("10.0.1.50", entry.sourceIP());
    assertEquals("REBALANCE", entry.operation());
    assertEquals("cluster:us-east", entry.resource());
    assertEquals("SUCCESS", entry.result());
    assertEquals(1800000, entry.durationMs());
    assertEquals(2, entry.parameters().size());
  }

  @Test
  public void testAuditLogEntryWithError() {
    AuditLogEntry entry = AuditLogEntry.builder()
        .requestId("req-456")
        .user("eve@example.com")
        .operation("STOP_EXECUTION")
        .result("DENIED")
        .errorMessage("User lacks permission: executions:stop:us-east")
        .build();

    assertEquals("req-456", entry.requestId());
    assertEquals("DENIED", entry.result());
    assertEquals("User lacks permission: executions:stop:us-east", entry.errorMessage());
  }

  @Test
  public void testAuditLogEntryWithMetadata() {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("brokers_affected", 50);
    metadata.put("partitions_moved", 1200);

    AuditLogEntry entry = AuditLogEntry.builder()
        .requestId("req-789")
        .user("charlie@example.com")
        .operation("REBALANCE")
        .result("SUCCESS")
        .metadata(metadata)
        .build();

    assertEquals(2, entry.metadata().size());
    assertEquals(50, entry.metadata().get("brokers_affected"));
    assertEquals(1200, entry.metadata().get("partitions_moved"));
  }

  @Test
  public void testAuditLogEntryToJson() {
    AuditLogEntry entry = AuditLogEntry.builder()
        .requestId("req-123")
        .user("alice@example.com")
        .operation("REBALANCE")
        .result("SUCCESS")
        .build();

    String json = entry.toCompactJson();
    assertNotNull(json);
    assertTrue(json.contains("req-123"));
    assertTrue(json.contains("alice@example.com"));
    assertTrue(json.contains("REBALANCE"));
    assertTrue(json.contains("SUCCESS"));
  }

  @Test
  public void testAuditLogEntryBuilder() {
    AuditLogEntry.Builder builder = AuditLogEntry.builder();
    builder.requestId("req-123")
           .user("alice@example.com")
           .operation("REBALANCE")
           .result("SUCCESS");

    AuditLogEntry entry = builder.build();
    assertNotNull(entry);
    assertEquals("req-123", entry.requestId());
  }

  @Test(expected = NullPointerException.class)
  public void testAuditLogEntryBuilderMissingRequestId() {
    AuditLogEntry.builder()
        .user("alice@example.com")
        .operation("REBALANCE")
        .result("SUCCESS")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testAuditLogEntryBuilderMissingUser() {
    AuditLogEntry.builder()
        .requestId("req-123")
        .operation("REBALANCE")
        .result("SUCCESS")
        .build();
  }

  @Test
  public void testAuditLogEntryParameterAddition() {
    AuditLogEntry entry = AuditLogEntry.builder()
        .requestId("req-123")
        .user("alice@example.com")
        .operation("REBALANCE")
        .result("SUCCESS")
        .parameter("key1", "value1")
        .parameter("key2", "value2")
        .build();

    assertEquals(2, entry.parameters().size());
    assertEquals("value1", entry.parameters().get("key1"));
    assertEquals("value2", entry.parameters().get("key2"));
  }

  @Test
  public void testAuditLogEntryMetadataAddition() {
    AuditLogEntry entry = AuditLogEntry.builder()
        .requestId("req-123")
        .user("alice@example.com")
        .operation("REBALANCE")
        .result("SUCCESS")
        .metadata("brokers", 50)
        .metadata("partitions", 1200)
        .build();

    assertEquals(2, entry.metadata().size());
    assertEquals(50, entry.metadata().get("brokers"));
    assertEquals(1200, entry.metadata().get("partitions"));
  }
}
