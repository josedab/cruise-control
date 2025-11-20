/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.state;

import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;

/**
 * Listener interface for execution state changes.
 */
public interface ExecutionStateListener {
  /**
   * Called when the execution state changes.
   *
   * @param oldState the previous state
   * @param newState the new state
   */
  void onStateChange(ExecutorState.State oldState, ExecutorState.State newState);
}
