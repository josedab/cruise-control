/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class RoleTest {

  @Test
  public void testRoleCreation() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ));
    permissions.add(new Permission(Resource.PROPOSALS, Action.READ));

    Role role = new Role("VIEWER", permissions);
    assertEquals("VIEWER", role.name());
    assertEquals(2, role.permissions().size());
  }

  @Test
  public void testRoleCreationWithSinglePermission() {
    Permission permission = new Permission(Resource.ADMIN, Action.ADMIN);
    Role role = new Role("ADMIN", permission);
    assertEquals("ADMIN", role.name());
    assertEquals(1, role.permissions().size());
    assertTrue(role.permissions().contains(permission));
  }

  @Test
  public void testRoleGrantsPermission() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ, "*"));

    Role role = new Role("VIEWER", permissions);
    Permission required = new Permission(Resource.STATE, Action.READ, "us-east");

    assertTrue(role.grants(required));
  }

  @Test
  public void testRoleDoesNotGrantPermission() {
    Set<Permission> permissions = new HashSet<>();
    permissions.add(new Permission(Resource.STATE, Action.READ, "*"));

    Role role = new Role("VIEWER", permissions);
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");

    assertFalse(role.grants(required));
  }

  @Test
  public void testRoleAddPermission() {
    Role role = new Role("TEST", new HashSet<>());
    assertEquals(0, role.permissions().size());

    role.addPermission(new Permission(Resource.STATE, Action.READ));
    assertEquals(1, role.permissions().size());
  }

  @Test
  public void testAdminRole() {
    Role admin = Role.admin();
    assertEquals("ADMIN", admin.name());

    // Admin should grant all permissions
    assertTrue(admin.grants(new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east")));
    assertTrue(admin.grants(new Permission(Resource.STATE, Action.READ, "*")));
    assertTrue(admin.grants(new Permission(Resource.CONFIG, Action.WRITE, "production")));
  }

  @Test
  public void testOperatorRole() {
    Role operator = Role.operator();
    assertEquals("OPERATOR", operator.name());

    // Operator should grant read and execute permissions
    assertTrue(operator.grants(new Permission(Resource.STATE, Action.READ, "*")));
    assertTrue(operator.grants(new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*")));
    assertTrue(operator.grants(new Permission(Resource.PROPOSALS, Action.WRITE, "*")));

    // Operator should not grant admin permissions
    assertFalse(operator.grants(new Permission(Resource.ADMIN, Action.ADMIN, "*")));
  }

  @Test
  public void testViewerRole() {
    Role viewer = Role.viewer();
    assertEquals("VIEWER", viewer.name());

    // Viewer should grant read permissions
    assertTrue(viewer.grants(new Permission(Resource.STATE, Action.READ, "*")));
    assertTrue(viewer.grants(new Permission(Resource.PROPOSALS, Action.READ, "*")));

    // Viewer should not grant execute permissions
    assertFalse(viewer.grants(new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*")));
  }

  @Test
  public void testExecutorRole() {
    Role executor = Role.executor();
    assertEquals("EXECUTOR", executor.name());

    // Executor should grant read and execute permissions
    assertTrue(executor.grants(new Permission(Resource.STATE, Action.READ, "*")));
    assertTrue(executor.grants(new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*")));

    // Executor should not grant proposal write permissions
    assertFalse(executor.grants(new Permission(Resource.PROPOSALS, Action.WRITE, "*")));
  }

  @Test
  public void testAuditorRole() {
    Role auditor = Role.auditor();
    assertEquals("AUDITOR", auditor.name());

    // Auditor should grant read permissions for state and audit logs
    assertTrue(auditor.grants(new Permission(Resource.STATE, Action.READ, "*")));
    assertTrue(auditor.grants(new Permission(Resource.AUDIT, Action.READ, "*")));

    // Auditor should not grant execute permissions
    assertFalse(auditor.grants(new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*")));
  }

  @Test
  public void testRoleEquality() {
    Role r1 = new Role("TEST", new HashSet<>());
    Role r2 = new Role("TEST", new HashSet<>());
    assertEquals(r1, r2);
    assertEquals(r1.hashCode(), r2.hashCode());
  }

  @Test
  public void testRoleInequality() {
    Role r1 = new Role("ADMIN", new HashSet<>());
    Role r2 = new Role("VIEWER", new HashSet<>());
    assertNotEquals(r1, r2);
  }
}
