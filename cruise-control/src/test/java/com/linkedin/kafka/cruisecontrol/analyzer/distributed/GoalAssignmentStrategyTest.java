/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for goal assignment strategies.
 */
public class GoalAssignmentStrategyTest {

    @Test
    public void testStaticAssignmentRoundRobin() {
        // Create mock goals
        List<Goal> goals = Arrays.asList(
                new RackAwareGoal(),
                new ReplicaCapacityGoal(),
                new DiskCapacityGoal(),
                new CpuCapacityGoal(),
                new NetworkInboundCapacityGoal()
        );

        // Create mock workers
        List<WorkerClient> workers = Arrays.asList(
                new WorkerClient("http://worker1:9091"),
                new WorkerClient("http://worker2:9091")
        );

        StaticGoalAssignmentStrategy strategy = new StaticGoalAssignmentStrategy();
        Map<WorkerClient, List<Goal>> assignments = strategy.assignGoals(goals, workers);

        // Verify all workers got assignments
        assertEquals(2, assignments.size());

        // Verify round-robin distribution (5 goals, 2 workers: 3 and 2)
        int totalAssigned = assignments.values().stream()
                .mapToInt(List::size)
                .sum();
        assertEquals(5, totalAssigned);

        // Verify goals are assigned
        assertTrue(assignments.get(workers.get(0)).size() >= 2);
        assertTrue(assignments.get(workers.get(1)).size() >= 2);
    }

    @Test
    public void testStaticAssignmentSingleWorker() {
        List<Goal> goals = Arrays.asList(
                new RackAwareGoal(),
                new ReplicaCapacityGoal()
        );

        List<WorkerClient> workers = Arrays.asList(
                new WorkerClient("http://worker1:9091")
        );

        StaticGoalAssignmentStrategy strategy = new StaticGoalAssignmentStrategy();
        Map<WorkerClient, List<Goal>> assignments = strategy.assignGoals(goals, workers);

        // Single worker should get all goals
        assertEquals(1, assignments.size());
        assertEquals(2, assignments.get(workers.get(0)).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStaticAssignmentNoWorkers() {
        List<Goal> goals = Arrays.asList(new RackAwareGoal());
        List<WorkerClient> workers = new ArrayList<>();

        StaticGoalAssignmentStrategy strategy = new StaticGoalAssignmentStrategy();
        strategy.assignGoals(goals, workers);
    }

    @Test
    public void testDynamicAssignment() {
        List<Goal> goals = Arrays.asList(
                new RackAwareGoal(),
                new ReplicaCapacityGoal(),
                new DiskCapacityGoal(),
                new CpuCapacityGoal()
        );

        List<WorkerClient> workers = Arrays.asList(
                new WorkerClient("http://worker1:9091"),
                new WorkerClient("http://worker2:9091")
        );

        DynamicGoalAssignmentStrategy strategy = new DynamicGoalAssignmentStrategy();
        Map<WorkerClient, List<Goal>> assignments = strategy.assignGoals(goals, workers);

        // Verify all workers got assignments
        assertEquals(2, assignments.size());

        // Verify all goals are assigned
        int totalAssigned = assignments.values().stream()
                .mapToInt(List::size)
                .sum();
        assertEquals(4, totalAssigned);
    }

    @Test
    public void testDynamicWorkQueue() {
        List<Goal> goals = Arrays.asList(
                new RackAwareGoal(),
                new ReplicaCapacityGoal(),
                new DiskCapacityGoal()
        );

        DynamicGoalAssignmentStrategy.WorkQueue workQueue =
                new DynamicGoalAssignmentStrategy.WorkQueue(goals);

        // Test work retrieval
        Goal goal1 = workQueue.getWork("worker1");
        assertNotNull(goal1);
        assertEquals(2, workQueue.remainingWork());

        Goal goal2 = workQueue.getWork("worker2");
        assertNotNull(goal2);
        assertEquals(1, workQueue.remainingWork());

        Goal goal3 = workQueue.getWork("worker1");
        assertNotNull(goal3);
        assertEquals(0, workQueue.remainingWork());

        // No more work available
        Goal goal4 = workQueue.getWork("worker1");
        assertNull(goal4);

        // Test work return
        workQueue.returnWork(goal1);
        assertEquals(1, workQueue.remainingWork());

        Goal retrievedGoal = workQueue.getWork("worker2");
        assertNotNull(retrievedGoal);
        assertEquals(0, workQueue.remainingWork());
    }

    @Test
    public void testStrategyNames() {
        StaticGoalAssignmentStrategy staticStrategy = new StaticGoalAssignmentStrategy();
        assertEquals("STATIC", staticStrategy.name());

        DynamicGoalAssignmentStrategy dynamicStrategy = new DynamicGoalAssignmentStrategy();
        assertEquals("DYNAMIC", dynamicStrategy.name());
    }
}
