/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.task;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

/**
 * Repository for execution tasks.
 *
 * <p>Provides efficient storage and querying of tasks by various criteria.
 * This class is thread-safe.
 */
public class ExecutionTaskRepository {
  // Primary storage: task ID -> task
  private final Map<Long, ExecutionTask> _tasksById = new ConcurrentHashMap<>();

  // Index: state -> set of task IDs
  private final Map<ExecutionTaskState, Set<Long>> _taskIdsByState = new ConcurrentHashMap<>();

  // Index: topic-partition -> task ID (for inter-broker movements)
  private final Map<TopicPartition, Long> _taskIdsByPartition = new ConcurrentHashMap<>();

  /**
   * Saves a task to the repository.
   *
   * @param task the task to save
   */
  public void save(ExecutionTask task) {
    if (task == null) {
      return;
    }

    Long taskId = task.executionId();
    _tasksById.put(taskId, task);

    // Update state index
    _taskIdsByState
        .computeIfAbsent(task.state(), k -> ConcurrentHashMap.newKeySet())
        .add(taskId);

    // Update partition index for inter-broker movements
    TopicPartition tp = task.proposal() != null ? task.proposal().topicPartition() : null;
    if (tp != null) {
      _taskIdsByPartition.put(tp, taskId);
    }
  }

  /**
   * Retrieves a task by ID.
   *
   * @param id the task ID
   * @return the task, or null if not found
   */
  public ExecutionTask get(Long id) {
    return _tasksById.get(id);
  }

  /**
   * Finds all tasks in a given state.
   *
   * @param state the execution state to filter by
   * @return list of tasks in that state
   */
  public List<ExecutionTask> findByState(ExecutionTaskState state) {
    Set<Long> taskIds = _taskIdsByState.getOrDefault(state, Collections.emptySet());

    return taskIds.stream()
        .map(_tasksById::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Finds a task by topic-partition.
   *
   * @param topicPartition the topic-partition to search for
   * @return the task, or null if not found
   */
  public ExecutionTask findByPartition(TopicPartition topicPartition) {
    Long taskId = _taskIdsByPartition.get(topicPartition);
    return taskId != null ? _tasksById.get(taskId) : null;
  }

  /**
   * Updates task state and maintains index consistency.
   *
   * @param taskId the task ID
   * @param newState the new state
   */
  public void updateState(Long taskId, ExecutionTaskState newState) {
    ExecutionTask task = _tasksById.get(taskId);
    if (task == null) {
      return;
    }

    ExecutionTaskState oldState = task.state();

    // Remove from old state index
    Set<Long> oldStateSet = _taskIdsByState.get(oldState);
    if (oldStateSet != null) {
      oldStateSet.remove(taskId);
    }

    // Update task state
    task.setState(newState);

    // Add to new state index
    _taskIdsByState
        .computeIfAbsent(newState, k -> ConcurrentHashMap.newKeySet())
        .add(taskId);
  }

  /**
   * Removes a task from the repository.
   *
   * @param taskId the task ID to remove
   * @return the removed task, or null if not found
   */
  public ExecutionTask remove(Long taskId) {
    ExecutionTask task = _tasksById.remove(taskId);
    if (task == null) {
      return null;
    }

    // Remove from state index
    Set<Long> stateSet = _taskIdsByState.get(task.state());
    if (stateSet != null) {
      stateSet.remove(taskId);
    }

    // Remove from partition index
    TopicPartition tp = task.proposal() != null ? task.proposal().topicPartition() : null;
    if (tp != null) {
      _taskIdsByPartition.remove(tp);
    }

    return task;
  }

  /**
   * Gets all tasks.
   *
   * @return list of all tasks
   */
  public List<ExecutionTask> getAllTasks() {
    return new ArrayList<>(_tasksById.values());
  }

  /**
   * Gets the total number of tasks.
   *
   * @return task count
   */
  public int size() {
    return _tasksById.size();
  }

  /**
   * Clears all tasks from the repository.
   */
  public void clear() {
    _tasksById.clear();
    _taskIdsByState.clear();
    _taskIdsByPartition.clear();
  }

  /**
   * Gets count of tasks in a specific state.
   *
   * @param state the state to count
   * @return number of tasks in that state
   */
  public int countByState(ExecutionTaskState state) {
    Set<Long> taskIds = _taskIdsByState.get(state);
    return taskIds != null ? taskIds.size() : 0;
  }

  /**
   * Checks if a task exists for the given partition.
   *
   * @param topicPartition the partition to check
   * @return true if a task exists for this partition
   */
  public boolean hasTaskForPartition(TopicPartition topicPartition) {
    return _taskIdsByPartition.containsKey(topicPartition);
  }
}
