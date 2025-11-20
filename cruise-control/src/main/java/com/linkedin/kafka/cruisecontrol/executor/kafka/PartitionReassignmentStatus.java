/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.kafka;

import java.util.List;

/**
 * Represents the status of a partition reassignment.
 */
public class PartitionReassignmentStatus {
  private final boolean _complete;
  private final List<Integer> _replicas;
  private final List<Integer> _addingReplicas;
  private final List<Integer> _removingReplicas;

  /**
   * Creates a new partition reassignment status.
   *
   * @param complete whether the reassignment is complete
   * @param replicas current replica list
   * @param addingReplicas replicas being added
   * @param removingReplicas replicas being removed
   */
  public PartitionReassignmentStatus(boolean complete,
                                    List<Integer> replicas,
                                    List<Integer> addingReplicas,
                                    List<Integer> removingReplicas) {
    _complete = complete;
    _replicas = replicas;
    _addingReplicas = addingReplicas;
    _removingReplicas = removingReplicas;
  }

  /**
   * @return true if the reassignment is complete
   */
  public boolean isComplete() {
    return _complete;
  }

  /**
   * @return the current replica list
   */
  public List<Integer> replicas() {
    return _replicas;
  }

  /**
   * @return the replicas being added
   */
  public List<Integer> addingReplicas() {
    return _addingReplicas;
  }

  /**
   * @return the replicas being removed
   */
  public List<Integer> removingReplicas() {
    return _removingReplicas;
  }

  /**
   * @return true if a reassignment is in progress
   */
  public boolean isInProgress() {
    return !_complete;
  }
}
