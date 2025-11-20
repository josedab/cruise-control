/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class AuthorizationServiceTest {

  private RBACProvider rbacProvider;
  private AuthorizationService authzService;

  @Before
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    config.put("rbac.users.admin", "alice@example.com");
    config.put("rbac.users.operator", "charlie@example.com");
    config.put("rbac.users.viewer", "eve@example.com");
    config.put("rbac.default.role", "viewer");

    rbacProvider = new FileBasedRBACProvider(config);
    authzService = new AuthorizationService(rbacProvider, true);
  }

  @Test
  public void testIsAuthorizedAdminUser() {
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(authzService.isAuthorized("alice@example.com", permission));
  }

  @Test
  public void testIsAuthorizedOperatorUser() {
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(authzService.isAuthorized("charlie@example.com", permission));
  }

  @Test
  public void testIsAuthorizedViewerUserReadPermission() {
    Permission permission = new Permission(Resource.STATE, Action.READ, "us-east");
    assertTrue(authzService.isAuthorized("eve@example.com", permission));
  }

  @Test
  public void testIsNotAuthorizedViewerUserExecutePermission() {
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertFalse(authzService.isAuthorized("eve@example.com", permission));
  }

  @Test
  public void testIsAuthorizedWithUserObject() {
    User user = new User("alice@example.com", Role.admin());
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(authzService.isAuthorized(user, permission));
  }

  @Test
  public void testRequirePermissionSuccess() throws UnauthorizedException {
    authzService.requirePermission("alice@example.com",
        new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east"));
    // Should not throw
  }

  @Test(expected = UnauthorizedException.class)
  public void testRequirePermissionFailure() throws UnauthorizedException {
    authzService.requirePermission("eve@example.com",
        new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east"));
  }

  @Test
  public void testIsAuthorizedWithRBACDisabled() {
    AuthorizationService disabledAuthzService = new AuthorizationService(rbacProvider, false);
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");

    // With RBAC disabled, all users should be authorized
    assertTrue(disabledAuthzService.isAuthorized("eve@example.com", permission));
    assertTrue(disabledAuthzService.isAuthorized("unknown@example.com", permission));
  }

  @Test
  public void testGetUserRoles() {
    Set<Role> roles = authzService.getUserRoles("alice@example.com");
    assertFalse(roles.isEmpty());
    assertTrue(roles.stream().anyMatch(r -> r.name().equals("ADMIN")));
  }

  @Test
  public void testGetUserPermissions() {
    Set<Permission> permissions = authzService.getUserPermissions("alice@example.com");
    assertFalse(permissions.isEmpty());

    // Admin should have admin:*:* permission
    assertTrue(permissions.stream().anyMatch(p ->
        p.resource() == Resource.ADMIN && p.action() == Action.ADMIN
    ));
  }

  @Test
  public void testIsAuthorizedNullUser() {
    Permission permission = new Permission(Resource.STATE, Action.READ, "*");
    assertFalse(authzService.isAuthorized((String) null, permission));
  }

  @Test
  public void testIsAuthorizedNullPermission() {
    assertFalse(authzService.isAuthorized("alice@example.com", null));
  }

  @Test(expected = UnauthorizedException.class)
  public void testRequirePermissionNullUser() throws UnauthorizedException {
    authzService.requirePermission((User) null, new Permission(Resource.STATE, Action.READ, "*"));
  }

  @Test
  public void testDefaultRoleForUnknownUser() {
    // Unknown user should get default role (viewer)
    Permission readPermission = new Permission(Resource.STATE, Action.READ, "*");
    assertTrue(authzService.isAuthorized("unknown@example.com", readPermission));

    Permission executePermission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*");
    assertFalse(authzService.isAuthorized("unknown@example.com", executePermission));
  }
}
