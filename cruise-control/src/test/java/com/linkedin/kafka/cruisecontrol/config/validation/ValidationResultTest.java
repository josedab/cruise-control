/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ValidationResult}.
 */
public class ValidationResultTest {

  @Test
  public void testEmptyResult() {
    ValidationResult result = new ValidationResult();

    assertTrue(result.isValid());
    assertFalse(result.hasErrors());
    assertFalse(result.hasWarnings());
    assertEquals(0, result.errorCount());
    assertEquals(0, result.warningCount());
    assertTrue(result.getAll().isEmpty());
  }

  @Test
  public void testResultWithErrors() {
    ValidationError error1 = ValidationError.error("prop1", "msg1", "sugg1");
    ValidationError error2 = ValidationError.error("prop2", "msg2", "sugg2");

    ValidationResult result = new ValidationResult(Arrays.asList(error1, error2));

    assertFalse(result.isValid());
    assertTrue(result.hasErrors());
    assertFalse(result.hasWarnings());
    assertEquals(2, result.errorCount());
    assertEquals(0, result.warningCount());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.getWarnings().isEmpty());
  }

  @Test
  public void testResultWithWarnings() {
    ValidationError warning1 = ValidationError.warning("prop1", "msg1", "sugg1");
    ValidationError warning2 = ValidationError.warning("prop2", "msg2", "sugg2");

    ValidationResult result = new ValidationResult(Arrays.asList(warning1, warning2));

    assertFalse(result.isValid());
    assertFalse(result.hasErrors());
    assertTrue(result.hasWarnings());
    assertEquals(0, result.errorCount());
    assertEquals(2, result.warningCount());
    assertTrue(result.getErrors().isEmpty());
    assertEquals(2, result.getWarnings().size());
  }

  @Test
  public void testResultWithMixed() {
    ValidationError error = ValidationError.error("prop1", "msg1", "sugg1");
    ValidationError warning = ValidationError.warning("prop2", "msg2", "sugg2");

    ValidationResult result = new ValidationResult(Arrays.asList(error, warning));

    assertFalse(result.isValid());
    assertTrue(result.hasErrors());
    assertTrue(result.hasWarnings());
    assertEquals(1, result.errorCount());
    assertEquals(1, result.warningCount());
    assertEquals(1, result.getErrors().size());
    assertEquals(1, result.getWarnings().size());
    assertEquals(2, result.getAll().size());
  }

  @Test
  public void testMerge() {
    ValidationError error1 = ValidationError.error("prop1", "msg1", "sugg1");
    ValidationError error2 = ValidationError.error("prop2", "msg2", "sugg2");

    ValidationResult result1 = new ValidationResult(Collections.singletonList(error1));
    ValidationResult result2 = new ValidationResult(Collections.singletonList(error2));

    result1.merge(result2);

    assertEquals(2, result1.errorCount());
    assertEquals(2, result1.getAll().size());
  }

  @Test
  public void testAddError() {
    ValidationResult result = new ValidationResult();
    ValidationError error = ValidationError.error("prop", "msg", "sugg");

    result.addError(error);

    assertTrue(result.hasErrors());
    assertEquals(1, result.errorCount());
  }

  @Test
  public void testToString() {
    ValidationResult valid = new ValidationResult();
    assertTrue(valid.toString().contains("0 errors"));
    assertTrue(valid.toString().contains("0 warnings"));

    ValidationError error = ValidationError.error("prop", "msg", "sugg");
    ValidationResult withError = new ValidationResult(Collections.singletonList(error));
    assertTrue(withError.toString().contains("1 errors"));
  }
}
