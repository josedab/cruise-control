/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an authenticated user in the RBAC system.
 * A user has an identity (username/email) and a set of roles.
 */
public class User implements Principal {
  private final String _identity;
  private final Set<Role> _roles;

  /**
   * Creates a new user with the specified identity and roles.
   *
   * @param identity the user's unique identifier (username or email)
   * @param roles the set of roles assigned to this user
   */
  public User(String identity, Set<Role> roles) {
    _identity = Objects.requireNonNull(identity, "User identity cannot be null");
    _roles = new HashSet<>(Objects.requireNonNull(roles, "Roles cannot be null"));
  }

  /**
   * Creates a new user with a single role.
   *
   * @param identity the user's unique identifier
   * @param role the role assigned to this user
   */
  public User(String identity, Role role) {
    this(identity, Collections.singleton(role));
  }

  /**
   * @return the user's unique identifier
   */
  public String identity() {
    return _identity;
  }

  /**
   * @return the user's name (same as identity for Principal interface)
   */
  @Override
  public String getName() {
    return _identity;
  }

  /**
   * @return an unmodifiable view of the roles assigned to this user
   */
  public Set<Role> roles() {
    return Collections.unmodifiableSet(_roles);
  }

  /**
   * Checks if this user has the specified permission.
   * A user has a permission if any of their roles grants it.
   *
   * @param permission the required permission
   * @return true if the user has the permission
   */
  public boolean hasPermission(Permission permission) {
    if (permission == null) {
      return false;
    }

    for (Role role : _roles) {
      if (role.grants(permission)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks if this user has a specific role by name.
   *
   * @param roleName the role name to check
   * @return true if the user has the role
   */
  public boolean hasRole(String roleName) {
    if (roleName == null) {
      return false;
    }

    for (Role role : _roles) {
      if (role.name().equalsIgnoreCase(roleName)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Adds a role to this user.
   *
   * @param role the role to add
   */
  public void addRole(Role role) {
    if (role != null) {
      _roles.add(role);
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
    User user = (User) o;
    return Objects.equals(_identity, user._identity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_identity);
  }

  @Override
  public String toString() {
    return "User{identity='" + _identity + "', roles=" + _roles.size() + "}";
  }
}
