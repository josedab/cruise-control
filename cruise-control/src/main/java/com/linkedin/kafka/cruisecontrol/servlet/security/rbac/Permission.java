/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a permission in the RBAC system.
 * A permission is defined by a resource, an action, and an optional cluster scope.
 *
 * Permission format: resource:action[:cluster]
 *
 * Examples:
 * - state:read:*                     # Read state of all clusters
 * - state:read:us-east               # Read state of us-east cluster only
 * - executions:execute:production-*  # Execute on all production clusters
 * - admin:*:*                        # Full admin access to all clusters
 */
public class Permission {
  private static final String WILDCARD = "*";
  private static final String PERMISSION_SEPARATOR = ":";

  private final Resource _resource;
  private final Action _action;
  private final String _cluster;

  /**
   * Creates a new permission.
   *
   * @param resource the resource this permission applies to
   * @param action the action this permission allows
   * @param cluster the cluster scope (use "*" for all clusters)
   */
  public Permission(Resource resource, Action action, String cluster) {
    _resource = Objects.requireNonNull(resource, "Resource cannot be null");
    _action = Objects.requireNonNull(action, "Action cannot be null");
    _cluster = cluster != null ? cluster : WILDCARD;
  }

  /**
   * Creates a permission that applies to all clusters.
   *
   * @param resource the resource
   * @param action the action
   */
  public Permission(Resource resource, Action action) {
    this(resource, action, WILDCARD);
  }

  /**
   * Parse a permission from its string representation.
   * Format: resource:action[:cluster]
   *
   * @param permissionString the permission string
   * @return the parsed Permission
   * @throws IllegalArgumentException if the format is invalid
   */
  public static Permission fromString(String permissionString) {
    if (permissionString == null || permissionString.trim().isEmpty()) {
      throw new IllegalArgumentException("Permission string cannot be null or empty");
    }

    String[] parts = permissionString.split(PERMISSION_SEPARATOR);
    if (parts.length < 2 || parts.length > 3) {
      throw new IllegalArgumentException(
          "Invalid permission format. Expected 'resource:action[:cluster]', got: " + permissionString);
    }

    Resource resource = Resource.fromString(parts[0]);
    Action action = Action.fromString(parts[1]);
    String cluster = parts.length == 3 ? parts[2] : WILDCARD;

    return new Permission(resource, action, cluster);
  }

  /**
   * @return the resource this permission applies to
   */
  public Resource resource() {
    return _resource;
  }

  /**
   * @return the action this permission allows
   */
  public Action action() {
    return _action;
  }

  /**
   * @return the cluster scope of this permission
   */
  public String cluster() {
    return _cluster;
  }

  /**
   * Checks if this permission grants the specified permission.
   * A permission grants another if:
   * - Resources match (or this has wildcard resource via ADMIN)
   * - Actions match (or this has wildcard action via ADMIN)
   * - Cluster scopes match (considering wildcards and patterns)
   *
   * @param required the permission to check
   * @return true if this permission grants the required permission
   */
  public boolean grants(Permission required) {
    if (required == null) {
      return false;
    }

    // Check resource match
    if (_resource != Resource.ADMIN && _resource != required._resource) {
      return false;
    }

    // Check action match
    if (_action != Action.ADMIN && _action != required._action) {
      return false;
    }

    // Check cluster match
    return matchesCluster(required._cluster);
  }

  /**
   * Checks if the cluster scope of this permission matches the specified cluster.
   *
   * @param targetCluster the target cluster to check
   * @return true if this permission's cluster scope matches the target
   */
  private boolean matchesCluster(String targetCluster) {
    if (WILDCARD.equals(_cluster)) {
      return true;
    }

    if (WILDCARD.equals(targetCluster)) {
      return WILDCARD.equals(_cluster);
    }

    // Exact match
    if (_cluster.equals(targetCluster)) {
      return true;
    }

    // Wildcard pattern match (e.g., "production-*" matches "production-us-east")
    if (_cluster.contains(WILDCARD)) {
      String pattern = _cluster.replace("*", ".*");
      return Pattern.matches(pattern, targetCluster);
    }

    return false;
  }

  /**
   * @return the string representation of this permission (resource:action:cluster)
   */
  @Override
  public String toString() {
    return _resource.resourceName() + PERMISSION_SEPARATOR
         + _action.actionName() + PERMISSION_SEPARATOR
         + _cluster;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Permission that = (Permission) o;
    return _resource == that._resource
        && _action == that._action
        && Objects.equals(_cluster, that._cluster);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_resource, _action, _cluster);
  }
}
