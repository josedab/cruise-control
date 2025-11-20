/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Dynamic goal assignment strategy using work stealing pattern.
 * Workers pull goals from a shared queue as they become available.
 *
 * <p>Pros:
 * - Automatic load balancing
 * - Adapts to varying goal execution times
 * - Better fault tolerance (failed work can be reassigned)
 *
 * <p>Cons:
 * - Requires coordination between workers
 * - More complex implementation
 * - Potential for increased coordination overhead
 */
public class DynamicGoalAssignmentStrategy implements GoalAssignmentStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicGoalAssignmentStrategy.class);
    private static final int INITIAL_GOALS_PER_WORKER = 2;

    @Override
    public Map<WorkerClient, List<Goal>> assignGoals(List<Goal> goals, List<WorkerClient> workers) {
        if (workers.isEmpty()) {
            throw new IllegalArgumentException("No workers available for goal assignment");
        }

        // For initial implementation, we assign a few goals to each worker upfront
        // In a full implementation, this would use a work queue that workers pull from
        Map<WorkerClient, List<Goal>> assignments = new HashMap<>();
        for (WorkerClient worker : workers) {
            assignments.put(worker, new ArrayList<>());
        }

        // Assign initial batch of goals to each worker
        int totalGoals = goals.size();
        int goalsPerWorker = Math.max(1, totalGoals / workers.size());

        int goalIndex = 0;
        for (WorkerClient worker : workers) {
            List<Goal> workerGoals = new ArrayList<>();
            for (int i = 0; i < goalsPerWorker && goalIndex < totalGoals; i++) {
                workerGoals.add(goals.get(goalIndex++));
            }
            assignments.put(worker, workerGoals);
        }

        // Assign remaining goals (if any) round-robin
        int workerIndex = 0;
        while (goalIndex < totalGoals) {
            WorkerClient worker = workers.get(workerIndex);
            assignments.get(worker).add(goals.get(goalIndex++));
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
        return "DYNAMIC";
    }

    /**
     * Work queue for dynamic goal distribution.
     * This would be used in a full implementation where workers pull goals as needed.
     */
    public static class WorkQueue {
        private final Queue<Goal> _pendingGoals;

        public WorkQueue(List<Goal> goals) {
            _pendingGoals = new ConcurrentLinkedQueue<>(goals);
        }

        /**
         * Get the next available goal for a worker.
         *
         * @param workerId The worker requesting work
         * @return The next goal, or null if no more work is available
         */
        public synchronized Goal getWork(String workerId) {
            if (_pendingGoals.isEmpty()) {
                return null;
            }
            Goal goal = _pendingGoals.poll();
            if (goal != null) {
                LOG.info("Assigned goal {} to worker {}", goal.getClass().getSimpleName(), workerId);
            }
            return goal;
        }

        /**
         * Return a goal to the queue (e.g., if a worker fails).
         */
        public synchronized void returnWork(Goal goal) {
            _pendingGoals.add(goal);
            LOG.info("Returned goal {} to work queue", goal.getClass().getSimpleName());
        }

        public int remainingWork() {
            return _pendingGoals.size();
        }
    }
}
