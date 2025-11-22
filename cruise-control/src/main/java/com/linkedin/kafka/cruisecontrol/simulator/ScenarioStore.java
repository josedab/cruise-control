/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.simulator.model.Scenario;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory store for simulation scenarios.
 * <p>
 * This class provides CRUD operations for saved scenarios.
 * For production use, this should be backed by persistent storage.
 * </p>
 */
public class ScenarioStore {
  private static final Logger LOG = LoggerFactory.getLogger(ScenarioStore.class);

  private final Map<String, SavedScenario> _scenarios;
  private final int _maxScenarios;

  /**
   * Constructor with default max scenarios.
   */
  public ScenarioStore() {
    this(100);
  }

  /**
   * Constructor with custom max scenarios.
   *
   * @param maxScenarios maximum number of scenarios to store
   */
  public ScenarioStore(int maxScenarios) {
    _scenarios = new ConcurrentHashMap<>();
    _maxScenarios = maxScenarios;
  }

  /**
   * Saves a scenario.
   *
   * @param scenario the scenario to save
   * @param description optional description
   * @param tags optional tags
   * @return saved scenario with generated ID
   */
  public SavedScenario save(Scenario scenario, String description, Set<String> tags) {
    // Enforce max scenarios limit
    if (_scenarios.size() >= _maxScenarios) {
      removeOldestScenario();
    }

    String id = UUID.randomUUID().toString();
    SavedScenario saved = new SavedScenario(
        id,
        scenario,
        description,
        tags != null ? tags : Collections.emptySet(),
        Instant.now(),
        null
    );

    _scenarios.put(id, saved);
    LOG.info("Saved scenario '{}' with ID {}", scenario.name(), id);

    return saved;
  }

  /**
   * Gets a scenario by ID.
   *
   * @param id scenario ID
   * @return optional saved scenario
   */
  public Optional<SavedScenario> get(String id) {
    return Optional.ofNullable(_scenarios.get(id));
  }

  /**
   * Gets all saved scenarios.
   *
   * @return list of saved scenarios
   */
  public List<SavedScenario> getAll() {
    return new ArrayList<>(_scenarios.values());
  }

  /**
   * Gets scenarios by tag.
   *
   * @param tag tag to filter by
   * @return list of matching scenarios
   */
  public List<SavedScenario> getByTag(String tag) {
    return _scenarios.values().stream()
        .filter(s -> s.tags().contains(tag))
        .collect(Collectors.toList());
  }

  /**
   * Updates the last run time of a scenario.
   *
   * @param id scenario ID
   */
  public void updateLastRun(String id) {
    SavedScenario existing = _scenarios.get(id);
    if (existing != null) {
      SavedScenario updated = new SavedScenario(
          existing.id(),
          existing.scenario(),
          existing.description(),
          existing.tags(),
          existing.createdAt(),
          Instant.now()
      );
      _scenarios.put(id, updated);
    }
  }

  /**
   * Deletes a scenario.
   *
   * @param id scenario ID
   * @return true if deleted, false if not found
   */
  public boolean delete(String id) {
    SavedScenario removed = _scenarios.remove(id);
    if (removed != null) {
      LOG.info("Deleted scenario '{}' with ID {}", removed.scenario().name(), id);
      return true;
    }
    return false;
  }

  /**
   * Gets the count of saved scenarios.
   *
   * @return scenario count
   */
  public int count() {
    return _scenarios.size();
  }

  /**
   * Removes the oldest scenario to make room for new ones.
   */
  private void removeOldestScenario() {
    _scenarios.entrySet().stream()
        .min((a, b) -> a.getValue().createdAt().compareTo(b.getValue().createdAt()))
        .ifPresent(oldest -> {
          _scenarios.remove(oldest.getKey());
          LOG.debug("Removed oldest scenario '{}' to make room", oldest.getValue().scenario().name());
        });
  }

  /**
   * Represents a saved scenario with metadata.
   */
  public static class SavedScenario {
    private final String _id;
    private final Scenario _scenario;
    private final String _description;
    private final Set<String> _tags;
    private final Instant _createdAt;
    private final Instant _lastRun;

    public SavedScenario(String id, Scenario scenario, String description,
                        Set<String> tags, Instant createdAt, Instant lastRun) {
      _id = id;
      _scenario = scenario;
      _description = description;
      _tags = Collections.unmodifiableSet(tags);
      _createdAt = createdAt;
      _lastRun = lastRun;
    }

    public String id() {
      return _id;
    }

    public Scenario scenario() {
      return _scenario;
    }

    public String description() {
      return _description;
    }

    public Set<String> tags() {
      return _tags;
    }

    public Instant createdAt() {
      return _createdAt;
    }

    public Instant lastRun() {
      return _lastRun;
    }

    @Override
    public String toString() {
      return String.format("SavedScenario{id='%s', name='%s', tags=%s, createdAt=%s}",
          _id, _scenario.name(), _tags, _createdAt);
    }
  }
}
