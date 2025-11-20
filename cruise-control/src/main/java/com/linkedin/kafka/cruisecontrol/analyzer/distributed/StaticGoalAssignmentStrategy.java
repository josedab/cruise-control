/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static goal assignment strategy that distributes goals evenly across workers
 * in a round-robin fashion.
 *
 * <p>Pros:
 * - Simple and predictable
 * - No coordination overhead
 * - Deterministic assignment
 *
 * <p>Cons:
 * - May be unbalanced if some goals take longer than others
 * - Cannot adapt to worker failures or varying performance
 */
public class StaticGoalAssignmentStrategy implements GoalAssignmentStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(StaticGoalAssignmentStrategy.class);

    @Override
    public Map<WorkerClient, List<Goal>> assignGoals(List<Goal> goals, List<WorkerClient> workers) {
        if (workers.isEmpty()) {
            throw new IllegalArgumentException("No workers available for goal assignment");
        }

        Map<WorkerClient, List<Goal>> assignments = new HashMap<>();
        for (WorkerClient worker : workers) {
            assignments.put(worker, new ArrayList<>());
        }

        // Round-robin assignment
        int workerIndex = 0;
        for (Goal goal : goals) {
            WorkerClient worker = workers.get(workerIndex);
            assignments.get(worker).add(goal);
            workerIndex = (workerIndex + 1) % workers.size();
        }

        // Log assignments
        for (Map.Entry<WorkerClient, List<Goal>> entry : assignments.entrySet()) {
            LOG.info("Assigned {} goals to worker {}: {}",
                    entry.getValue().size(),
                    entry.getKey().getWorkerUrl(),
                    entry.getValue().stream().map(g -> g.getClass().getSimpleName()).toArray());
        }

        return assignments;
    }

    @Override
    public String name() {
        return "STATIC";
    }
}
