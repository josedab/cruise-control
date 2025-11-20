/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

/**
 * Represents a resource in the RBAC system that can be protected by permissions.
 * Resources represent different components of Cruise Control that users may interact with.
 */
public enum Resource {
  /**
   * All clusters or specific cluster operations
   */
  CLUSTERS("clusters"),

  /**
   * Broker-specific operations
   */
  BROKERS("brokers"),

  /**
   * Partition-level operations
   */
  PARTITIONS("partitions"),

  /**
   * Optimization proposal operations
   */
  PROPOSALS("proposals"),

  /**
   * Execution operations (rebalance, broker addition/removal, etc.)
   */
  EXECUTIONS("executions"),

  /**
   * Configuration management
   */
  CONFIG("config"),

  /**
   * Cluster state queries
   */
  STATE("state"),

  /**
   * Administrative operations
   */
  ADMIN("admin"),

  /**
   * Audit log access
   */
  AUDIT("audit");

  private final String _name;

  Resource(String name) {
    _name = name;
  }

  /**
   * @return the string representation of this resource
   */
  public String resourceName() {
    return _name;
  }

  /**
   * Parse a resource from its string representation.
   *
   * @param name the resource name
   * @return the corresponding Resource enum value
   * @throws IllegalArgumentException if the name doesn't match any resource
   */
  public static Resource fromString(String name) {
    for (Resource resource : Resource.values()) {
      if (resource._name.equalsIgnoreCase(name)) {
        return resource;
      }
    }
    throw new IllegalArgumentException("Unknown resource: " + name);
  }

  @Override
  public String toString() {
    return _name;
  }
}
