/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

/**
 * Represents an action that can be performed on a resource in the RBAC system.
 */
public enum Action {
  /**
   * Read/view operations (GET requests, state queries)
   */
  READ("read"),

  /**
   * Write/modify operations (configuration changes, proposal modifications)
   */
  WRITE("write"),

  /**
   * Execute operations (trigger rebalance, start/stop operations)
   */
  EXECUTE("execute"),

  /**
   * Administrative actions (system configuration, user management)
   */
  ADMIN("admin");

  private final String _name;

  Action(String name) {
    _name = name;
  }

  /**
   * @return the string representation of this action
   */
  public String actionName() {
    return _name;
  }

  /**
   * Parse an action from its string representation.
   *
   * @param name the action name
   * @return the corresponding Action enum value
   * @throws IllegalArgumentException if the name doesn't match any action
   */
  public static Action fromString(String name) {
    for (Action action : Action.values()) {
      if (action._name.equalsIgnoreCase(name)) {
        return action;
      }
    }
    throw new IllegalArgumentException("Unknown action: " + name);
  }

  @Override
  public String toString() {
    return _name;
  }
}
