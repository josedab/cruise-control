/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * Handles authorization decisions based on RBAC policies.
 * This service checks whether users have the required permissions to perform operations.
 */
public class AuthorizationService {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationService.class);

  private final RBACProvider _rbacProvider;
  private final boolean _enabled;

  /**
   * Creates a new authorization service.
   *
   * @param rbacProvider the RBAC provider for role lookups
   * @param enabled whether RBAC is enabled
   */
  public AuthorizationService(RBACProvider rbacProvider, boolean enabled) {
    _rbacProvider = Objects.requireNonNull(rbacProvider, "RBAC provider cannot be null");
    _enabled = enabled;
  }

  /**
   * @return true if RBAC is enabled
   */
  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Checks if a user has permission to perform an action.
   *
   * @param userIdentity the authenticated user identity
   * @param permission the required permission
   * @return true if authorized, false otherwise
   */
  public boolean isAuthorized(String userIdentity, Permission permission) {
    if (!_enabled) {
      // RBAC disabled - allow all authenticated users
      return true;
    }

    if (userIdentity == null || permission == null) {
      return false;
    }

    // Get user's roles
    Set<Role> roles = _rbacProvider.getRolesByIdentity(userIdentity);

    if (roles.isEmpty()) {
      LOG.debug("User {} has no roles assigned", userIdentity);
      return false;
    }

    // Check if any role grants the permission
    for (Role role : roles) {
      if (role.grants(permission)) {
        LOG.debug("User {} authorized for permission {} via role {}",
                  userIdentity, permission, role.name());
        return true;
      }
    }

    LOG.debug("User {} denied permission {}", userIdentity, permission);
    return false;
  }

  /**
   * Checks if a user object has permission to perform an action.
   *
   * @param user the authenticated user
   * @param permission the required permission
   * @return true if authorized, false otherwise
   */
  public boolean isAuthorized(User user, Permission permission) {
    if (user == null) {
      return false;
    }
    return isAuthorized(user.identity(), permission);
  }

  /**
   * Enforces authorization, throwing an exception if the user lacks permission.
   *
   * @param userIdentity the authenticated user identity
   * @param permission the required permission
   * @throws UnauthorizedException if the user lacks permission
   */
  public void requirePermission(String userIdentity, Permission permission) throws UnauthorizedException {
    if (!isAuthorized(userIdentity, permission)) {
      Set<Role> roles = _rbacProvider.getRolesByIdentity(userIdentity);
      throw new UnauthorizedException(
          String.format("User %s lacks permission: %s (current roles: %s)",
                       userIdentity,
                       permission,
                       formatRoles(roles))
      );
    }
  }

  /**
   * Enforces authorization for a user object.
   *
   * @param user the authenticated user
   * @param permission the required permission
   * @throws UnauthorizedException if the user lacks permission
   */
  public void requirePermission(User user, Permission permission) throws UnauthorizedException {
    if (user == null) {
      throw new UnauthorizedException("User cannot be null");
    }
    requirePermission(user.identity(), permission);
  }

  /**
   * Gets the roles assigned to a user.
   *
   * @param userIdentity the user identity
   * @return the set of roles assigned to the user
   */
  public Set<Role> getUserRoles(String userIdentity) {
    return _rbacProvider.getRolesByIdentity(userIdentity);
  }

  /**
   * Gets all permissions granted to a user across all their roles.
   *
   * @param userIdentity the user identity
   * @return the set of all permissions granted to the user
   */
  public Set<Permission> getUserPermissions(String userIdentity) {
    Set<Role> roles = _rbacProvider.getRolesByIdentity(userIdentity);
    Set<Permission> allPermissions = new java.util.HashSet<>();

    for (Role role : roles) {
      allPermissions.addAll(role.permissions());
    }

    return allPermissions;
  }

  private String formatRoles(Set<Role> roles) {
    if (roles.isEmpty()) {
      return "none";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Role role : roles) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(role.name());
      first = false;
    }
    return sb.toString();
  }
}
