/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.state;

import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.*;

/**
 * Enforces valid state transitions for execution.
 *
 * <p>Valid transitions:
 * <pre>
 *   NO_TASK_IN_PROGRESS → STARTING_EXECUTION
 *   STARTING_EXECUTION → INITIALIZING_PROPOSAL_EXECUTION
 *   INITIALIZING_PROPOSAL_EXECUTION → GENERATING_PROPOSALS_FOR_EXECUTION
 *   GENERATING_PROPOSALS_FOR_EXECUTION → INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS
 *   INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS → INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS
 *   INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS → LEADER_MOVEMENT_TASK_IN_PROGRESS
 *   INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS → LEADER_MOVEMENT_TASK_IN_PROGRESS
 *   LEADER_MOVEMENT_TASK_IN_PROGRESS → NO_TASK_IN_PROGRESS
 *   * → STOPPING_EXECUTION (any state can be stopped)
 *   STOPPING_EXECUTION → NO_TASK_IN_PROGRESS
 * </pre>
 */
public class ExecutionStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionStateMachine.class);

  private volatile ExecutorState.State _currentState = NO_TASK_IN_PROGRESS;
  private final List<ExecutionStateListener> _listeners = new ArrayList<>();

  /**
   * Transitions to a new state if valid.
   *
   * @param newState the desired state
   * @throws IllegalStateTransitionException if transition is invalid
   */
  public synchronized void transition(ExecutorState.State newState) {
    if (!isValidTransition(_currentState, newState)) {
      String message = String.format("Cannot transition from %s to %s", _currentState, newState);
      LOG.error(message);
      throw new IllegalStateTransitionException(message);
    }

    ExecutorState.State oldState = _currentState;
    _currentState = newState;

    LOG.info("State transition: {} -> {}", oldState, newState);

    // Notify listeners
    notifyListeners(oldState, newState);
  }

  /**
   * Attempts to transition to a new state, returning success status.
   *
   * @param newState the desired state
   * @return true if transition succeeded, false otherwise
   */
  public synchronized boolean tryTransition(ExecutorState.State newState) {
    if (!isValidTransition(_currentState, newState)) {
      LOG.warn("Invalid state transition from {} to {}", _currentState, newState);
      return false;
    }

    transition(newState);
    return true;
  }

  /**
   * Checks if a transition from one state to another is valid.
   *
   * @param from the current state
   * @param to the desired state
   * @return true if transition is valid
   */
  private boolean isValidTransition(ExecutorState.State from, ExecutorState.State to) {
    // Allow staying in same state
    if (from == to) {
      return true;
    }

    // Stopping can happen from any state
    if (to == STOPPING_EXECUTION) {
      return true;
    }

    // State transition matrix
    return switch (from) {
      case NO_TASK_IN_PROGRESS ->
          to == STARTING_EXECUTION;

      case STARTING_EXECUTION ->
          to == INITIALIZING_PROPOSAL_EXECUTION ||
          to == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == LEADER_MOVEMENT_TASK_IN_PROGRESS;

      case INITIALIZING_PROPOSAL_EXECUTION ->
          to == GENERATING_PROPOSALS_FOR_EXECUTION ||
          to == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == LEADER_MOVEMENT_TASK_IN_PROGRESS;

      case GENERATING_PROPOSALS_FOR_EXECUTION ->
          to == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == LEADER_MOVEMENT_TASK_IN_PROGRESS;

      case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ->
          to == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ||
          to == LEADER_MOVEMENT_TASK_IN_PROGRESS ||
          to == NO_TASK_IN_PROGRESS;

      case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS ->
          to == LEADER_MOVEMENT_TASK_IN_PROGRESS ||
          to == NO_TASK_IN_PROGRESS;

      case LEADER_MOVEMENT_TASK_IN_PROGRESS ->
          to == NO_TASK_IN_PROGRESS;

      case STOPPING_EXECUTION ->
          to == NO_TASK_IN_PROGRESS;

      default -> false;
    };
  }

  /**
   * Gets the current state.
   *
   * @return the current state
   */
  public ExecutorState.State currentState() {
    return _currentState;
  }

  /**
   * Checks if execution is in progress.
   *
   * @return true if in an in-progress state
   */
  public boolean isExecutionInProgress() {
    return ExecutorState.IN_PROGRESS_STATES.contains(_currentState);
  }

  /**
   * Registers a listener to be notified of state changes.
   *
   * @param listener the listener to register
   */
  public synchronized void registerListener(ExecutionStateListener listener) {
    if (!_listeners.contains(listener)) {
      _listeners.add(listener);
    }
  }

  /**
   * Unregisters a listener.
   *
   * @param listener the listener to unregister
   */
  public synchronized void unregisterListener(ExecutionStateListener listener) {
    _listeners.remove(listener);
  }

  /**
   * Notifies all registered listeners of a state change.
   */
  private void notifyListeners(ExecutorState.State oldState, ExecutorState.State newState) {
    for (ExecutionStateListener listener : _listeners) {
      try {
        listener.onStateChange(oldState, newState);
      } catch (Exception e) {
        LOG.error("Error notifying listener of state change", e);
      }
    }
  }

  /**
   * Resets the state machine to initial state.
   */
  public synchronized void reset() {
    ExecutorState.State oldState = _currentState;
    _currentState = NO_TASK_IN_PROGRESS;
    LOG.info("State machine reset from {} to {}", oldState, _currentState);
    notifyListeners(oldState, _currentState);
  }
}
