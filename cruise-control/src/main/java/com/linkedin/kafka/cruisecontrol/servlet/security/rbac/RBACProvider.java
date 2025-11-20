/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import java.util.Set;

/**
 * Interface for RBAC providers that manage role assignments to users.
 * Implementations can load role mappings from various sources (files, LDAP, database, etc.).
 */
public interface RBACProvider {
  /**
   * Gets the roles assigned to a user.
   *
   * @param user the user to look up
   * @return the set of roles assigned to the user (empty set if no roles are assigned)
   */
  Set<Role> getRoles(User user);

  /**
   * Gets the roles assigned to a user by identity.
   *
   * @param userIdentity the user's unique identifier
   * @return the set of roles assigned to the user (empty set if no roles are assigned)
   */
  Set<Role> getRolesByIdentity(String userIdentity);

  /**
   * Checks if a user has a specific role.
   *
   * @param userIdentity the user's unique identifier
   * @param roleName the role name to check
   * @return true if the user has the role
   */
  boolean hasRole(String userIdentity, String roleName);

  /**
   * Gets all defined roles in the system.
   *
   * @return the set of all roles
   */
  Set<Role> getAllRoles();
}
