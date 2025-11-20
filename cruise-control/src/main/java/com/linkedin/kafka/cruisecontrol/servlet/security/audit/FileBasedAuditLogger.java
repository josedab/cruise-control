/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.audit;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * File-based audit logger that writes audit entries to a JSON lines file.
 * Each line in the file is a complete JSON object representing one audit log entry.
 */
public class FileBasedAuditLogger implements AuditLogger {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedAuditLogger.class);
  private static final Gson GSON = new Gson();

  private final String _logFilePath;
  private final BufferedWriter _writer;

  /**
   * Creates a new file-based audit logger.
   *
   * @param logFilePath the path to the audit log file
   * @throws IOException if the file cannot be opened for writing
   */
  public FileBasedAuditLogger(String logFilePath) throws IOException {
    _logFilePath = logFilePath;

    // Ensure parent directory exists
    File logFile = new File(logFilePath);
    File parentDir = logFile.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new IOException("Failed to create audit log directory: " + parentDir.getAbsolutePath());
      }
    }

    // Open file in append mode
    _writer = new BufferedWriter(new FileWriter(logFile, true));

    LOG.info("Audit logging enabled. Writing to: {}", _logFilePath);
  }

  @Override
  public synchronized void log(AuditLogEntry entry) {
    if (entry == null) {
      return;
    }

    try {
      _writer.write(entry.toCompactJson());
      _writer.newLine();
      _writer.flush();
    } catch (IOException e) {
      LOG.error("Failed to write audit log entry", e);
    }
  }

  @Override
  public List<AuditLogEntry> query(Map<String, String> filters) {
    List<AuditLogEntry> results = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(_logFilePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        try {
          AuditLogEntry entry = parseEntry(line);
          if (entry != null && matchesFilters(entry, filters)) {
            results.add(entry);
          }
        } catch (Exception e) {
          LOG.warn("Failed to parse audit log line: {}", line, e);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to query audit logs", e);
    }

    return results;
  }

  @Override
  public List<AuditLogEntry> queryByTimeRange(long startTimeMs, long endTimeMs, int limit) {
    List<AuditLogEntry> results = new ArrayList<>();
    Instant start = Instant.ofEpochMilli(startTimeMs);
    Instant end = Instant.ofEpochMilli(endTimeMs);

    try (BufferedReader reader = new BufferedReader(new FileReader(_logFilePath))) {
      String line;
      while ((line = reader.readLine()) != null && results.size() < limit) {
        try {
          AuditLogEntry entry = parseEntry(line);
          if (entry != null && isInTimeRange(entry, start, end)) {
            results.add(entry);
          }
        } catch (Exception e) {
          LOG.warn("Failed to parse audit log line: {}", line, e);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to query audit logs by time range", e);
    }

    return results;
  }

  @Override
  public void flush() {
    try {
      _writer.flush();
    } catch (IOException e) {
      LOG.error("Failed to flush audit log", e);
    }
  }

  @Override
  public void close() {
    try {
      _writer.close();
      LOG.info("Audit logger closed");
    } catch (IOException e) {
      LOG.error("Failed to close audit logger", e);
    }
  }

  private AuditLogEntry parseEntry(String json) {
    try {
      // Parse the JSON back to get the data
      @SuppressWarnings("unchecked")
      Map<String, Object> data = GSON.fromJson(json, Map.class);

      AuditLogEntry.Builder builder = AuditLogEntry.builder()
          .requestId(getString(data, "_requestId"))
          .user(getString(data, "_user"))
          .operation(getString(data, "_operation"))
          .result(getString(data, "_result"));

      // Optional fields
      if (data.containsKey("_timestamp")) {
        String timestampStr = getString(data, "_timestamp");
        if (timestampStr != null) {
          builder.timestamp(Instant.parse(timestampStr));
        }
      }

      if (data.containsKey("_sourceIP")) {
        builder.sourceIP(getString(data, "_sourceIP"));
      }

      if (data.containsKey("_resource")) {
        builder.resource(getString(data, "_resource"));
      }

      if (data.containsKey("_errorMessage")) {
        builder.errorMessage(getString(data, "_errorMessage"));
      }

      if (data.containsKey("_durationMs")) {
        builder.durationMs(getLong(data, "_durationMs"));
      }

      if (data.containsKey("_parameters")) {
        @SuppressWarnings("unchecked")
        Map<String, String> params = (Map<String, String>) data.get("_parameters");
        if (params != null) {
          builder.parameters(params);
        }
      }

      if (data.containsKey("_metadata")) {
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) data.get("_metadata");
        if (metadata != null) {
          builder.metadata(metadata);
        }
      }

      return builder.build();
    } catch (Exception e) {
      LOG.warn("Failed to parse audit entry from JSON: {}", json, e);
      return null;
    }
  }

  private String getString(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value != null ? value.toString() : null;
  }

  private long getLong(Map<String, Object> map, String key) {
    Object value = map.get(key);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return 0;
  }

  private boolean matchesFilters(AuditLogEntry entry, Map<String, String> filters) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }

    for (Map.Entry<String, String> filter : filters.entrySet()) {
      String key = filter.getKey();
      String value = filter.getValue();

      switch (key) {
        case "user":
          if (!value.equals(entry.user())) {
            return false;
          }
          break;
        case "operation":
          if (!value.equals(entry.operation())) {
            return false;
          }
          break;
        case "result":
          if (!value.equals(entry.result())) {
            return false;
          }
          break;
        case "resource":
          if (!value.equals(entry.resource())) {
            return false;
          }
          break;
        default:
          // Ignore unknown filters
          break;
      }
    }

    return true;
  }

  private boolean isInTimeRange(AuditLogEntry entry, Instant start, Instant end) {
    Instant timestamp = entry.timestamp();
    if (timestamp == null) {
      return false;
    }
    return !timestamp.isBefore(start) && !timestamp.isAfter(end);
  }
}
