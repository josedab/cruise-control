/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;

import java.util.List;
import java.util.Map;

/**
 * Strategy for assigning goals to workers in distributed optimization.
 */
public interface GoalAssignmentStrategy {

    /**
     * Assign goals to available workers.
     *
     * @param goals List of goals to assign (in priority order)
     * @param workers List of available worker clients
     * @return Map of worker to assigned goals
     */
    Map<WorkerClient, List<Goal>> assignGoals(List<Goal> goals, List<WorkerClient> workers);

    /**
     * Get the name of this strategy.
     */
    String name();
}
