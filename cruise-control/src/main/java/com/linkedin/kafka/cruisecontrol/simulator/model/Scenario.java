/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator.model;

import java.util.Collections;
import java.util.List;

/**
 * Represents a simulation scenario with a name, modifications, and failures.
 */
public class Scenario {
  private final String _name;
  private final Modifications _modifications;
  private final List<Failure> _failures;

  /**
   * Constructor for Scenario.
   *
   * @param name scenario name
   * @param modifications cluster modifications to apply
   * @param failures failures to simulate
   */
  public Scenario(String name, Modifications modifications, List<Failure> failures) {
    _name = name;
    _modifications = modifications != null ? modifications : new Modifications();
    _failures = failures != null ? Collections.unmodifiableList(failures) : Collections.emptyList();
  }

  /**
   * Constructor for Scenario without failures.
   *
   * @param name scenario name
   * @param modifications cluster modifications to apply
   */
  public Scenario(String name, Modifications modifications) {
    this(name, modifications, Collections.emptyList());
  }

  public String name() {
    return _name;
  }

  public Modifications modifications() {
    return _modifications;
  }

  public List<Failure> failures() {
    return _failures;
  }

  @Override
  public String toString() {
    return String.format("Scenario{name='%s', modifications=%s, failures=%d}",
        _name, _modifications, _failures.size());
  }
}
