/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import java.util.List;

/**
 * Formats validation results into human-readable reports.
 * Provides clear, actionable error messages with suggestions for fixes.
 */
public class ValidationReportFormatter {

  private static final String SEPARATOR = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
  private static final String HEADER_TOP = "╔═══════════════════════════════════════════════════════════╗";
  private static final String HEADER_BOTTOM = "╚═══════════════════════════════════════════════════════════╝";
  private static final int MAX_LINE_WIDTH = 80;

  /**
   * Formats a validation result into a detailed report.
   *
   * @param result The validation result to format
   * @return Formatted report string
   */
  public String format(ValidationResult result) {
    StringBuilder report = new StringBuilder();

    // Header
    report.append("\n").append(HEADER_TOP).append("\n");
    report.append("║       Cruise Control Configuration Validation             ║\n");
    report.append(HEADER_BOTTOM).append("\n\n");

    // Status
    if (result.isValid()) {
      report.append("Status: ✅ PASSED (0 errors, 0 warnings)\n\n");
      report.append("Configuration is valid!\n");
      return report.toString();
    }

    String statusIcon = result.hasErrors() ? "❌" : "⚠️";
    String statusText = result.hasErrors() ? "FAILED" : "PASSED WITH WARNINGS";
    report.append(String.format("Status: %s %s (%d errors, %d warnings)\n\n",
        statusIcon, statusText, result.errorCount(), result.warningCount()));

    report.append(SEPARATOR).append("\n\n");

    // Errors
    List<ValidationError> errors = result.getErrors();
    if (!errors.isEmpty()) {
      for (int i = 0; i < errors.size(); i++) {
        ValidationError error = errors.get(i);
        report.append(formatError(error, i + 1));
        report.append(SEPARATOR).append("\n\n");
      }
    }

    // Warnings
    List<ValidationError> warnings = result.getWarnings();
    if (!warnings.isEmpty()) {
      for (int i = 0; i < warnings.size(); i++) {
        ValidationError warning = warnings.get(i);
        report.append(formatWarning(warning, i + 1));
        report.append(SEPARATOR).append("\n\n");
      }
    }

    // Footer
    if (result.hasErrors()) {
      report.append("❌ Configuration validation failed. Fix errors above before starting.\n\n");
    } else {
      report.append("⚠️  Configuration has warnings. Review them before proceeding.\n\n");
    }

    report.append("For detailed documentation, see:\n");
    report.append("https://github.com/linkedin/cruise-control/wiki/Configuration\n");

    return report.toString();
  }

  /**
   * Formats a single error with details.
   */
  private String formatError(ValidationError error, int number) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("❌ ERROR #%d: %s\n", number, error.getProperty()));
    sb.append(String.format("   Property: %s\n", error.getProperty()));
    sb.append(String.format("   Issue: %s\n", error.getMessage()));

    if (error.getSuggestion() != null && !error.getSuggestion().isEmpty()) {
      sb.append(String.format("   Fix: %s\n", wrapText(error.getSuggestion(), 8)));
    }

    sb.append("\n");
    return sb.toString();
  }

  /**
   * Formats a single warning with details.
   */
  private String formatWarning(ValidationError warning, int number) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("⚠️  WARNING #%d: %s\n", number, warning.getProperty()));
    sb.append(String.format("   Property: %s\n", warning.getProperty()));
    sb.append(String.format("   Issue: %s\n", warning.getMessage()));

    if (warning.getSuggestion() != null && !warning.getSuggestion().isEmpty()) {
      sb.append(String.format("   Suggestion: %s\n", wrapText(warning.getSuggestion(), 15)));
    }

    sb.append("\n");
    return sb.toString();
  }

  /**
   * Wraps text to fit within the report width.
   */
  private String wrapText(String text, int indent) {
    if (text == null || text.isEmpty()) {
      return "";
    }

    int maxWidth = MAX_LINE_WIDTH - indent;
    if (text.length() <= maxWidth) {
      return text;
    }

    StringBuilder wrapped = new StringBuilder();
    String[] words = text.split("\\s+");
    int lineLength = 0;

    for (String word : words) {
      if (lineLength + word.length() + 1 > maxWidth) {
        wrapped.append("\n");
        for (int i = 0; i < indent; i++) {
          wrapped.append(" ");
        }
        wrapped.append(word);
        lineLength = word.length();
      } else {
        if (lineLength > 0) {
          wrapped.append(" ");
          lineLength++;
        }
        wrapped.append(word);
        lineLength += word.length();
      }
    }

    return wrapped.toString();
  }

  /**
   * Formats a validation result into a compact summary.
   *
   * @param result The validation result to format
   * @return Compact summary string
   */
  public String formatSummary(ValidationResult result) {
    if (result.isValid()) {
      return "✅ Configuration validation passed";
    }

    return String.format("%s Configuration validation: %d errors, %d warnings",
        result.hasErrors() ? "❌" : "⚠️",
        result.errorCount(),
        result.warningCount());
  }

  /**
   * Formats a validation result as JSON for API responses.
   *
   * @param result The validation result to format
   * @return JSON string representation
   */
  public String formatJson(ValidationResult result) {
    StringBuilder json = new StringBuilder();
    json.append("{\n");
    json.append("  \"status\": \"").append(result.hasErrors() ? "FAILED" : "PASSED").append("\",\n");
    json.append("  \"errorCount\": ").append(result.errorCount()).append(",\n");
    json.append("  \"warningCount\": ").append(result.warningCount()).append(",\n");

    // Errors
    json.append("  \"errors\": [\n");
    List<ValidationError> errors = result.getErrors();
    for (int i = 0; i < errors.size(); i++) {
      ValidationError error = errors.get(i);
      json.append("    {\n");
      json.append("      \"property\": \"").append(escapeJson(error.getProperty())).append("\",\n");
      json.append("      \"severity\": \"ERROR\",\n");
      json.append("      \"message\": \"").append(escapeJson(error.getMessage())).append("\",\n");
      json.append("      \"suggestion\": \"").append(escapeJson(error.getSuggestion())).append("\"\n");
      json.append("    }").append(i < errors.size() - 1 ? "," : "").append("\n");
    }
    json.append("  ],\n");

    // Warnings
    json.append("  \"warnings\": [\n");
    List<ValidationError> warnings = result.getWarnings();
    for (int i = 0; i < warnings.size(); i++) {
      ValidationError warning = warnings.get(i);
      json.append("    {\n");
      json.append("      \"property\": \"").append(escapeJson(warning.getProperty())).append("\",\n");
      json.append("      \"severity\": \"WARNING\",\n");
      json.append("      \"message\": \"").append(escapeJson(warning.getMessage())).append("\",\n");
      json.append("      \"suggestion\": \"").append(escapeJson(warning.getSuggestion())).append("\"\n");
      json.append("    }").append(i < warnings.size() - 1 ? "," : "").append("\n");
    }
    json.append("  ]\n");

    json.append("}\n");
    return json.toString();
  }

  /**
   * Escapes special characters for JSON.
   */
  private String escapeJson(String str) {
    if (str == null) {
      return "";
    }
    return str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
