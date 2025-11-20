/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ValidationError}.
 */
public class ValidationErrorTest {

  @Test
  public void testErrorCreation() {
    ValidationError error = ValidationError.error("test.property", "Test message", "Test suggestion");

    assertEquals("test.property", error.getProperty());
    assertEquals("Test message", error.getMessage());
    assertEquals("Test suggestion", error.getSuggestion());
    assertEquals(ValidationError.Severity.ERROR, error.getSeverity());
    assertTrue(error.isError());
    assertFalse(error.isWarning());
  }

  @Test
  public void testWarningCreation() {
    ValidationError warning = ValidationError.warning("test.property", "Test warning", "Test suggestion");

    assertEquals("test.property", warning.getProperty());
    assertEquals("Test warning", warning.getMessage());
    assertEquals("Test suggestion", warning.getSuggestion());
    assertEquals(ValidationError.Severity.WARNING, warning.getSeverity());
    assertFalse(warning.isError());
    assertTrue(warning.isWarning());
  }

  @Test
  public void testEquality() {
    ValidationError error1 = ValidationError.error("prop", "msg", "sugg");
    ValidationError error2 = ValidationError.error("prop", "msg", "sugg");
    ValidationError error3 = ValidationError.error("prop", "different", "sugg");

    assertEquals(error1, error2);
    assertNotEquals(error1, error3);
  }

  @Test
  public void testToString() {
    ValidationError error = ValidationError.error("test.property", "Test message", "Test suggestion");
    String str = error.toString();

    assertTrue(str.contains("ERROR"));
    assertTrue(str.contains("test.property"));
    assertTrue(str.contains("Test message"));
    assertTrue(str.contains("Test suggestion"));
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperty() {
    new ValidationError(null, ValidationError.Severity.ERROR, "message", "suggestion");
  }

  @Test(expected = NullPointerException.class)
  public void testNullSeverity() {
    new ValidationError("property", null, "message", "suggestion");
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessage() {
    new ValidationError("property", ValidationError.Severity.ERROR, null, "suggestion");
  }

  @Test
  public void testNullSuggestion() {
    ValidationError error = new ValidationError("property", ValidationError.Severity.ERROR, "message", null);
    assertEquals("", error.getSuggestion());
  }
}
