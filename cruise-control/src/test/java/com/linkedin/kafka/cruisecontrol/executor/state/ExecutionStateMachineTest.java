/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.state;

import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ExecutionStateMachine}.
 */
public class ExecutionStateMachineTest {

  @Test
  public void testValidTransitions() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Initial state should be NO_TASK_IN_PROGRESS
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());

    // Valid: NO_TASK_IN_PROGRESS -> STARTING_EXECUTION
    stateMachine.transition(STARTING_EXECUTION);
    assertEquals(STARTING_EXECUTION, stateMachine.currentState());

    // Valid: STARTING_EXECUTION -> INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS
    stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);
    assertEquals(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, stateMachine.currentState());

    // Valid: INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS -> LEADER_MOVEMENT_TASK_IN_PROGRESS
    stateMachine.transition(LEADER_MOVEMENT_TASK_IN_PROGRESS);
    assertEquals(LEADER_MOVEMENT_TASK_IN_PROGRESS, stateMachine.currentState());

    // Valid: LEADER_MOVEMENT_TASK_IN_PROGRESS -> NO_TASK_IN_PROGRESS
    stateMachine.transition(NO_TASK_IN_PROGRESS);
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());
  }

  @Test
  public void testInvalidTransition() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Initial state: NO_TASK_IN_PROGRESS
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());

    // Invalid: NO_TASK_IN_PROGRESS -> LEADER_MOVEMENT_TASK_IN_PROGRESS (skipping steps)
    try {
      stateMachine.transition(LEADER_MOVEMENT_TASK_IN_PROGRESS);
      fail("Should have thrown IllegalStateTransitionException");
    } catch (IllegalStateTransitionException e) {
      // Expected
      assertTrue(e.getMessage().contains("Cannot transition"));
    }

    // State should remain unchanged
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());
  }

  @Test
  public void testStoppingFromAnyState() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Can stop from NO_TASK_IN_PROGRESS
    stateMachine.transition(STOPPING_EXECUTION);
    assertEquals(STOPPING_EXECUTION, stateMachine.currentState());

    // Reset for next test
    stateMachine.reset();

    // Can stop from STARTING_EXECUTION
    stateMachine.transition(STARTING_EXECUTION);
    stateMachine.transition(STOPPING_EXECUTION);
    assertEquals(STOPPING_EXECUTION, stateMachine.currentState());

    // Reset and test from in-progress state
    stateMachine.reset();
    stateMachine.transition(STARTING_EXECUTION);
    stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);
    stateMachine.transition(STOPPING_EXECUTION);
    assertEquals(STOPPING_EXECUTION, stateMachine.currentState());
  }

  @Test
  public void testTryTransition() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Valid transition should succeed
    assertTrue(stateMachine.tryTransition(STARTING_EXECUTION));
    assertEquals(STARTING_EXECUTION, stateMachine.currentState());

    // Invalid transition should fail but not throw
    assertFalse(stateMachine.tryTransition(NO_TASK_IN_PROGRESS));
    assertEquals(STARTING_EXECUTION, stateMachine.currentState());
  }

  @Test
  public void testStateChangeListener() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Track state changes
    final ExecutorState.State[] capturedOldState = new ExecutorState.State[1];
    final ExecutorState.State[] capturedNewState = new ExecutorState.State[1];

    ExecutionStateListener listener = (oldState, newState) -> {
      capturedOldState[0] = oldState;
      capturedNewState[0] = newState;
    };

    stateMachine.registerListener(listener);

    // Trigger a transition
    stateMachine.transition(STARTING_EXECUTION);

    // Verify listener was called
    assertEquals(NO_TASK_IN_PROGRESS, capturedOldState[0]);
    assertEquals(STARTING_EXECUTION, capturedNewState[0]);
  }

  @Test
  public void testIsExecutionInProgress() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Initially not in progress
    assertFalse(stateMachine.isExecutionInProgress());

    // Start execution
    stateMachine.transition(STARTING_EXECUTION);
    assertFalse(stateMachine.isExecutionInProgress());

    // Move to in-progress state
    stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);
    assertTrue(stateMachine.isExecutionInProgress());

    // Stopping is also in-progress
    stateMachine.transition(STOPPING_EXECUTION);
    assertTrue(stateMachine.isExecutionInProgress());

    // Complete
    stateMachine.transition(NO_TASK_IN_PROGRESS);
    assertFalse(stateMachine.isExecutionInProgress());
  }

  @Test
  public void testReset() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Move to some state
    stateMachine.transition(STARTING_EXECUTION);
    stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);

    // Reset should go back to initial state
    stateMachine.reset();
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());
  }

  @Test
  public void testSameStateTransition() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Transitioning to same state should be valid
    stateMachine.transition(NO_TASK_IN_PROGRESS);
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());
  }

  @Test
  public void testCompleteExecutionFlow() {
    ExecutionStateMachine stateMachine = new ExecutionStateMachine();

    // Complete flow: NO_TASK -> STARTING -> INTER_BROKER -> INTRA_BROKER -> LEADER -> NO_TASK
    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());

    stateMachine.transition(STARTING_EXECUTION);
    stateMachine.transition(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);
    stateMachine.transition(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);
    stateMachine.transition(LEADER_MOVEMENT_TASK_IN_PROGRESS);
    stateMachine.transition(NO_TASK_IN_PROGRESS);

    assertEquals(NO_TASK_IN_PROGRESS, stateMachine.currentState());
  }
}
