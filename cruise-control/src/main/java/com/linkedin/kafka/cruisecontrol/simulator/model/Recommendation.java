/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

/**
 * A recommendation generated from simulation analysis.
 */
public class Recommendation {
  private final Severity _severity;
  private final String _title;
  private final String _description;

  /**
   * Constructor for Recommendation.
   *
   * @param severity the severity level
   * @param title brief title of the recommendation
   * @param description detailed description
   */
  public Recommendation(Severity severity, String title, String description) {
    _severity = severity;
    _title = title;
    _description = description;
  }

  public Severity severity() {
    return _severity;
  }

  public String title() {
    return _title;
  }

  public String description() {
    return _description;
  }

  @Override
  public String toString() {
    return String.format("Recommendation{severity=%s, title='%s', description='%s'}", _severity, _title, _description);
  }
}
