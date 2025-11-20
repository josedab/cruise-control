/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.rbac;

import org.junit.Test;

import static org.junit.Assert.*;

public class PermissionTest {

  @Test
  public void testPermissionCreation() {
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertEquals(Resource.EXECUTIONS, permission.resource());
    assertEquals(Action.EXECUTE, permission.action());
    assertEquals("us-east", permission.cluster());
  }

  @Test
  public void testPermissionCreationWithWildcard() {
    Permission permission = new Permission(Resource.STATE, Action.READ);
    assertEquals(Resource.STATE, permission.resource());
    assertEquals(Action.READ, permission.action());
    assertEquals("*", permission.cluster());
  }

  @Test
  public void testPermissionFromString() {
    Permission permission = Permission.fromString("executions:execute:us-east");
    assertEquals(Resource.EXECUTIONS, permission.resource());
    assertEquals(Action.EXECUTE, permission.action());
    assertEquals("us-east", permission.cluster());
  }

  @Test
  public void testPermissionFromStringWithWildcard() {
    Permission permission = Permission.fromString("state:read:*");
    assertEquals(Resource.STATE, permission.resource());
    assertEquals(Action.READ, permission.action());
    assertEquals("*", permission.cluster());
  }

  @Test
  public void testPermissionFromStringWithoutCluster() {
    Permission permission = Permission.fromString("state:read");
    assertEquals(Resource.STATE, permission.resource());
    assertEquals(Action.READ, permission.action());
    assertEquals("*", permission.cluster());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPermissionFromStringInvalidFormat() {
    Permission.fromString("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPermissionFromStringNull() {
    Permission.fromString(null);
  }

  @Test
  public void testPermissionToString() {
    Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertEquals("executions:execute:us-east", permission.toString());
  }

  @Test
  public void testPermissionGrantsExactMatch() {
    Permission granted = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(granted.grants(required));
  }

  @Test
  public void testPermissionGrantsWildcardCluster() {
    Permission granted = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(granted.grants(required));
  }

  @Test
  public void testPermissionGrantsWildcardPattern() {
    Permission granted = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "production-*");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "production-us-east");
    assertTrue(granted.grants(required));
  }

  @Test
  public void testPermissionDoesNotGrantDifferentResource() {
    Permission granted = new Permission(Resource.STATE, Action.READ, "*");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*");
    assertFalse(granted.grants(required));
  }

  @Test
  public void testPermissionDoesNotGrantDifferentAction() {
    Permission granted = new Permission(Resource.EXECUTIONS, Action.READ, "*");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "*");
    assertFalse(granted.grants(required));
  }

  @Test
  public void testPermissionDoesNotGrantDifferentCluster() {
    Permission granted = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-west");
    assertFalse(granted.grants(required));
  }

  @Test
  public void testAdminPermissionGrantsEverything() {
    Permission granted = new Permission(Resource.ADMIN, Action.ADMIN, "*");
    Permission required = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertTrue(granted.grants(required));
  }

  @Test
  public void testPermissionEquality() {
    Permission p1 = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    Permission p2 = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
  }

  @Test
  public void testPermissionInequality() {
    Permission p1 = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
    Permission p2 = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-west");
    assertNotEquals(p1, p2);
  }
}
