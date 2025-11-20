/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * File-based RBAC provider that loads role assignments from a properties file.
 *
 * Configuration format:
 * <pre>
 * # User-role mappings
 * rbac.users.admin=alice@example.com,bob@example.com
 * rbac.role.admin=admin:*:*
 *
 * rbac.users.operator=charlie@example.com,diana@example.com
 * rbac.role.operator=state:read:*,proposals:write:*,executions:execute:*
 *
 * rbac.users.viewer=eve@example.com
 * rbac.role.viewer=state:read:*,proposals:read:*
 * </pre>
 */
public class FileBasedRBACProvider implements RBACProvider {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedRBACProvider.class);

  private static final String RBAC_USERS_PREFIX = "rbac.users.";
  private static final String RBAC_ROLE_PREFIX = "rbac.role.";
  private static final String RBAC_DEFAULT_ROLE_CONFIG = "rbac.default.role";
  private static final String USER_SEPARATOR = ",";
  private static final String PERMISSION_SEPARATOR = ",";

  private final Map<String, Set<Role>> _userRoles;
  private final Map<String, Role> _roleDefinitions;
  private final Role _defaultRole;

  /**
   * Creates a file-based RBAC provider from a Cruise Control configuration.
   *
   * @param config the Cruise Control configuration
   */
  public FileBasedRBACProvider(KafkaCruiseControlConfig config) {
    _userRoles = new HashMap<>();
    _roleDefinitions = new HashMap<>();
    _defaultRole = loadDefaultRole(config);

    // Load built-in roles
    loadBuiltInRoles();

    // Load custom roles and user mappings from config
    loadFromConfig(config);
  }

  /**
   * Creates a file-based RBAC provider from a properties map.
   *
   * @param properties the RBAC configuration properties
   */
  public FileBasedRBACProvider(Map<String, String> properties) {
    _userRoles = new HashMap<>();
    _roleDefinitions = new HashMap<>();
    _defaultRole = loadDefaultRoleFromProperties(properties);

    // Load built-in roles
    loadBuiltInRoles();

    // Load custom roles and user mappings
    loadFromProperties(properties);
  }

  private void loadBuiltInRoles() {
    _roleDefinitions.put("admin", Role.admin());
    _roleDefinitions.put("operator", Role.operator());
    _roleDefinitions.put("viewer", Role.viewer());
    _roleDefinitions.put("executor", Role.executor());
    _roleDefinitions.put("auditor", Role.auditor());
  }

  private Role loadDefaultRole(KafkaCruiseControlConfig config) {
    String defaultRoleName = config.getString(RBAC_DEFAULT_ROLE_CONFIG);
    if (defaultRoleName == null || defaultRoleName.isEmpty()) {
      return Role.viewer(); // Safe default: read-only
    }
    return getBuiltInRole(defaultRoleName.toLowerCase());
  }

  private Role loadDefaultRoleFromProperties(Map<String, String> properties) {
    String defaultRoleName = properties.get(RBAC_DEFAULT_ROLE_CONFIG);
    if (defaultRoleName == null || defaultRoleName.isEmpty()) {
      return Role.viewer(); // Safe default: read-only
    }
    return getBuiltInRole(defaultRoleName.toLowerCase());
  }

  private Role getBuiltInRole(String roleName) {
    switch (roleName.toLowerCase()) {
      case "admin":
        return Role.admin();
      case "operator":
        return Role.operator();
      case "viewer":
        return Role.viewer();
      case "executor":
        return Role.executor();
      case "auditor":
        return Role.auditor();
      default:
        LOG.warn("Unknown default role: {}. Using VIEWER role instead.", roleName);
        return Role.viewer();
    }
  }

  private void loadFromConfig(KafkaCruiseControlConfig config) {
    Map<String, String> configMap = config.originals();
    loadFromProperties(configMap);
  }

  private void loadFromProperties(Map<String, String> properties) {
    // First, load custom role definitions
    Map<String, List<Permission>> customRoles = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(RBAC_ROLE_PREFIX)) {
        String roleName = key.substring(RBAC_ROLE_PREFIX.length());
        String permissionsStr = entry.getValue();
        List<Permission> permissions = parsePermissions(permissionsStr);
        customRoles.put(roleName, permissions);

        // Create and store the role
        Role role = new Role(roleName.toUpperCase(), new HashSet<>(permissions));
        _roleDefinitions.put(roleName.toLowerCase(), role);
      }
    }

    // Then, load user-role mappings
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(RBAC_USERS_PREFIX)) {
        String roleName = key.substring(RBAC_USERS_PREFIX.length());
        String usersStr = entry.getValue();
        String[] users = usersStr.split(USER_SEPARATOR);

        Role role = _roleDefinitions.get(roleName.toLowerCase());
        if (role == null) {
          LOG.warn("Role {} not defined. Users {} will not have this role.", roleName, usersStr);
          continue;
        }

        for (String user : users) {
          String trimmedUser = user.trim();
          if (!trimmedUser.isEmpty()) {
            _userRoles.computeIfAbsent(trimmedUser, k -> new HashSet<>()).add(role);
          }
        }
      }
    }

    LOG.info("Loaded RBAC configuration: {} roles, {} user mappings", _roleDefinitions.size(), _userRoles.size());
  }

  private List<Permission> parsePermissions(String permissionsStr) {
    List<Permission> permissions = new ArrayList<>();
    if (permissionsStr == null || permissionsStr.trim().isEmpty()) {
      return permissions;
    }

    String[] permissionStrings = permissionsStr.split(PERMISSION_SEPARATOR);
    for (String permStr : permissionStrings) {
      try {
        Permission permission = Permission.fromString(permStr.trim());
        permissions.add(permission);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid permission format: {}. Skipping.", permStr, e);
      }
    }

    return permissions;
  }

  @Override
  public Set<Role> getRoles(User user) {
    if (user == null) {
      return Collections.emptySet();
    }
    return getRolesByIdentity(user.identity());
  }

  @Override
  public Set<Role> getRolesByIdentity(String userIdentity) {
    if (userIdentity == null || userIdentity.isEmpty()) {
      return Collections.emptySet();
    }

    Set<Role> roles = _userRoles.get(userIdentity);
    if (roles != null && !roles.isEmpty()) {
      return Collections.unmodifiableSet(roles);
    }

    // Return default role if no specific roles are assigned
    if (_defaultRole != null) {
      return Collections.singleton(_defaultRole);
    }

    return Collections.emptySet();
  }

  @Override
  public boolean hasRole(String userIdentity, String roleName) {
    if (userIdentity == null || roleName == null) {
      return false;
    }

    Set<Role> roles = getRolesByIdentity(userIdentity);
    for (Role role : roles) {
      if (role.name().equalsIgnoreCase(roleName)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Set<Role> getAllRoles() {
    return Collections.unmodifiableSet(new HashSet<>(_roleDefinitions.values()));
  }

  /**
   * Gets a role definition by name.
   *
   * @param roleName the role name
   * @return the role, or null if not found
   */
  public Role getRole(String roleName) {
    if (roleName == null) {
      return null;
    }
    return _roleDefinitions.get(roleName.toLowerCase());
  }
}
