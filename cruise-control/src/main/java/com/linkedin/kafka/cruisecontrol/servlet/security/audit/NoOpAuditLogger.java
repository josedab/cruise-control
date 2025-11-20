/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.audit;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * No-op audit logger that discards all audit entries.
 * Used when audit logging is disabled.
 */
public class NoOpAuditLogger implements AuditLogger {
  @Override
  public void log(AuditLogEntry entry) {
    // No-op
  }

  @Override
  public List<AuditLogEntry> query(Map<String, String> filters) {
    return Collections.emptyList();
  }

  @Override
  public List<AuditLogEntry> queryByTimeRange(long startTimeMs, long endTimeMs, int limit) {
    return Collections.emptyList();
  }

  @Override
  public void flush() {
    // No-op
  }

  @Override
  public void close() {
    // No-op
  }
}
