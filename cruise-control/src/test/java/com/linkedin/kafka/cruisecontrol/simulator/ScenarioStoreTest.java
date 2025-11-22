/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.simulator.model.Modifications;
import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ScenarioStore.
 */
public class ScenarioStoreTest {

  private ScenarioStore _store;

  @Before
  public void setUp() {
    _store = new ScenarioStore(10);
  }

  @Test
  public void testSaveAndRetrieve() {
    Scenario scenario = new Scenario("Test Scenario", new Modifications());
    Set<String> tags = new HashSet<>();
    tags.add("test");
    tags.add("capacity-planning");

    ScenarioStore.SavedScenario saved = _store.save(scenario, "A test scenario", tags);

    assertNotNull("Saved scenario should not be null", saved);
    assertNotNull("ID should be generated", saved.id());
    assertEquals("Name should match", "Test Scenario", saved.scenario().name());
    assertEquals("Description should match", "A test scenario", saved.description());
    assertTrue("Tags should contain 'test'", saved.tags().contains("test"));

    // Retrieve by ID
    Optional<ScenarioStore.SavedScenario> retrieved = _store.get(saved.id());
    assertTrue("Should find scenario by ID", retrieved.isPresent());
    assertEquals("Retrieved scenario should match", saved.id(), retrieved.get().id());
  }

  @Test
  public void testGetByTag() {
    // Save scenarios with different tags
    Set<String> capacityTags = new HashSet<>();
    capacityTags.add("capacity-planning");
    _store.save(new Scenario("Capacity Test", new Modifications()), "desc", capacityTags);

    Set<String> drTags = new HashSet<>();
    drTags.add("disaster-recovery");
    _store.save(new Scenario("DR Test", new Modifications()), "desc", drTags);

    Set<String> bothTags = new HashSet<>();
    bothTags.add("capacity-planning");
    bothTags.add("disaster-recovery");
    _store.save(new Scenario("Both Test", new Modifications()), "desc", bothTags);

    // Query by tag
    List<ScenarioStore.SavedScenario> capacityScenarios = _store.getByTag("capacity-planning");
    assertEquals("Should find 2 capacity-planning scenarios", 2, capacityScenarios.size());

    List<ScenarioStore.SavedScenario> drScenarios = _store.getByTag("disaster-recovery");
    assertEquals("Should find 2 disaster-recovery scenarios", 2, drScenarios.size());
  }

  @Test
  public void testDelete() {
    Scenario scenario = new Scenario("Delete Test", new Modifications());
    ScenarioStore.SavedScenario saved = _store.save(scenario, "desc", null);
    String id = saved.id();

    assertTrue("Should find scenario before delete", _store.get(id).isPresent());

    boolean deleted = _store.delete(id);
    assertTrue("Delete should return true", deleted);
    assertFalse("Should not find scenario after delete", _store.get(id).isPresent());

    // Delete non-existent
    boolean deletedAgain = _store.delete(id);
    assertFalse("Delete of non-existent should return false", deletedAgain);
  }

  @Test
  public void testMaxScenariosLimit() {
    ScenarioStore limitedStore = new ScenarioStore(3);

    // Save 4 scenarios (exceeds limit of 3)
    limitedStore.save(new Scenario("Scenario 1", new Modifications()), "desc", null);
    limitedStore.save(new Scenario("Scenario 2", new Modifications()), "desc", null);
    limitedStore.save(new Scenario("Scenario 3", new Modifications()), "desc", null);
    limitedStore.save(new Scenario("Scenario 4", new Modifications()), "desc", null);

    // Should only have 3 scenarios
    assertEquals("Should have max 3 scenarios", 3, limitedStore.count());
  }

  @Test
  public void testUpdateLastRun() throws InterruptedException {
    Scenario scenario = new Scenario("Run Test", new Modifications());
    ScenarioStore.SavedScenario saved = _store.save(scenario, "desc", null);

    assertNull("LastRun should be null initially", saved.lastRun());

    // Wait a bit to ensure time difference
    Thread.sleep(10);
    _store.updateLastRun(saved.id());

    Optional<ScenarioStore.SavedScenario> updated = _store.get(saved.id());
    assertTrue("Should find updated scenario", updated.isPresent());
    assertNotNull("LastRun should be set", updated.get().lastRun());
    assertTrue("LastRun should be after createdAt",
        updated.get().lastRun().isAfter(updated.get().createdAt()));
  }

  @Test
  public void testGetAll() {
    _store.save(new Scenario("Scenario A", new Modifications()), "desc", null);
    _store.save(new Scenario("Scenario B", new Modifications()), "desc", null);
    _store.save(new Scenario("Scenario C", new Modifications()), "desc", null);

    List<ScenarioStore.SavedScenario> all = _store.getAll();
    assertEquals("Should return all 3 scenarios", 3, all.size());
  }
}
