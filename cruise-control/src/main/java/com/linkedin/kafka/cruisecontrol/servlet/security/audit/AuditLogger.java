/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.audit;

import java.util.List;
import java.util.Map;

/**
 * Interface for audit logging implementations.
 * Implementations can write audit logs to various destinations (files, Kafka, external systems).
 */
public interface AuditLogger {
  /**
   * Logs an audit entry.
   *
   * @param entry the audit log entry to write
   */
  void log(AuditLogEntry entry);

  /**
   * Queries audit logs based on filter criteria.
   *
   * @param filters map of filter criteria (e.g., "user", "operation", "result")
   * @return list of matching audit log entries
   */
  List<AuditLogEntry> query(Map<String, String> filters);

  /**
   * Queries audit logs within a time range.
   *
   * @param startTimeMs start time in milliseconds since epoch
   * @param endTimeMs end time in milliseconds since epoch
   * @param limit maximum number of entries to return
   * @return list of audit log entries within the time range
   */
  List<AuditLogEntry> queryByTimeRange(long startTimeMs, long endTimeMs, int limit);

  /**
   * Flushes any buffered audit log entries.
   */
  void flush();

  /**
   * Closes the audit logger and releases resources.
   */
  void close();
}
