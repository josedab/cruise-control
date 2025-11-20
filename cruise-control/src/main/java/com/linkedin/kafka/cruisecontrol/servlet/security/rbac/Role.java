/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a role in the RBAC system.
 * A role is a named collection of permissions that can be assigned to users.
 */
public class Role {
  private final String _name;
  private final Set<Permission> _permissions;

  /**
   * Creates a new role with the specified name and permissions.
   *
   * @param name the role name
   * @param permissions the set of permissions granted by this role
   */
  public Role(String name, Set<Permission> permissions) {
    _name = Objects.requireNonNull(name, "Role name cannot be null");
    _permissions = new HashSet<>(Objects.requireNonNull(permissions, "Permissions cannot be null"));
  }

  /**
   * Creates a new role with a single permission.
   *
   * @param name the role name
   * @param permission the permission granted by this role
   */
  public Role(String name, Permission permission) {
    this(name, Collections.singleton(permission));
  }

  /**
   * @return the role name
   */
  public String name() {
    return _name;
  }

  /**
   * @return an unmodifiable view of the permissions granted by this role
   */
  public Set<Permission> permissions() {
    return Collections.unmodifiableSet(_permissions);
  }

  /**
   * Checks if this role grants the specified permission.
   *
   * @param required the required permission
   * @return true if any of this role's permissions grants the required permission
   */
  public boolean grants(Permission required) {
    if (required == null) {
      return false;
    }

    for (Permission permission : _permissions) {
      if (permission.grants(required)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Adds a permission to this role.
   *
   * @param permission the permission to add
   */
  public void addPermission(Permission permission) {
    if (permission != null) {
      _permissions.add(permission);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return Objects.equals(_name, role._name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name);
  }

  @Override
  public String toString() {
    return "Role{name='" + _name + "', permissions=" + _permissions.size() + "}";
  }

  // Built-in roles factory methods

  /**
   * Creates the ADMIN role with full access to all resources and actions.
   *
   * @return the ADMIN role
   */
  public static Role admin() {
    return new Role("ADMIN", new Permission(Resource.ADMIN, Action.ADMIN));
  }

  /**
   * Creates the OPERATOR role with read and execute permissions.
   *
   * @return the OPERATOR role
   */
  public static Role operator() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ));
    permissions.add(new Permission(Resource.PROPOSALS, Action.READ));
    permissions.add(new Permission(Resource.PROPOSALS, Action.WRITE));
    permissions.add(new Permission(Resource.EXECUTIONS, Action.READ));
    permissions.add(new Permission(Resource.EXECUTIONS, Action.EXECUTE));
    permissions.add(new Permission(Resource.CONFIG, Action.READ));
    permissions.add(new Permission(Resource.BROKERS, Action.READ));
    return new Role("OPERATOR", permissions);
  }

  /**
   * Creates the VIEWER role with read-only access.
   *
   * @return the VIEWER role
   */
  public static Role viewer() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ));
    permissions.add(new Permission(Resource.PROPOSALS, Action.READ));
    permissions.add(new Permission(Resource.EXECUTIONS, Action.READ));
    permissions.add(new Permission(Resource.CONFIG, Action.READ));
    permissions.add(new Permission(Resource.BROKERS, Action.READ));
    permissions.add(new Permission(Resource.PARTITIONS, Action.READ));
    return new Role("VIEWER", permissions);
  }

  /**
   * Creates the EXECUTOR role with read and execute permissions (no proposal modification).
   *
   * @return the EXECUTOR role
   */
  public static Role executor() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ));
    permissions.add(new Permission(Resource.PROPOSALS, Action.READ));
    permissions.add(new Permission(Resource.EXECUTIONS, Action.READ));
    permissions.add(new Permission(Resource.EXECUTIONS, Action.EXECUTE));
    return new Role("EXECUTOR", permissions);
  }

  /**
   * Creates the AUDITOR role with read access to state and audit logs.
   *
   * @return the AUDITOR role
   */
  public static Role auditor() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ));
    permissions.add(new Permission(Resource.AUDIT, Action.READ));
    return new Role("AUDITOR", permissions);
  }
}
