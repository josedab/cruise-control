/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for DistributedGoalOptimizer.
 */
public class DistributedGoalOptimizerTest {

    @Test
    public void testOptimizerConfiguration() {
        List<String> workerUrls = Arrays.asList(
                "http://worker1:9091",
                "http://worker2:9091",
                "http://worker3:9091"
        );

        GoalAssignmentStrategy strategy = new StaticGoalAssignmentStrategy();
        DistributedGoalOptimizer optimizer = new DistributedGoalOptimizer(
                workerUrls,
                strategy,
                true,
                600000
        );

        assertTrue(optimizer.isEnabled());
        assertEquals(3, optimizer.getWorkerCount());
        assertEquals("STATIC", optimizer.getAssignmentStrategy());
    }

    @Test
    public void testOptimizerDisabled() {
        List<String> workerUrls = Collections.emptyList();
        GoalAssignmentStrategy strategy = new StaticGoalAssignmentStrategy();

        DistributedGoalOptimizer optimizer = new DistributedGoalOptimizer(
                workerUrls,
                strategy,
                false,
                600000
        );

        assertFalse(optimizer.isEnabled());
        assertEquals(0, optimizer.getWorkerCount());
    }

    @Test
    public void testOptimizerWithDynamicStrategy() {
        List<String> workerUrls = Arrays.asList(
                "http://worker1:9091",
                "http://worker2:9091"
        );

        GoalAssignmentStrategy strategy = new DynamicGoalAssignmentStrategy();
        DistributedGoalOptimizer optimizer = new DistributedGoalOptimizer(
                workerUrls,
                strategy,
                true,
                300000
        );

        assertTrue(optimizer.isEnabled());
        assertEquals(2, optimizer.getWorkerCount());
        assertEquals("DYNAMIC", optimizer.getAssignmentStrategy());
    }

    @Test(expected = IllegalStateException.class)
    public void testOptimizeWhenDisabled() throws Exception {
        DistributedGoalOptimizer optimizer = new DistributedGoalOptimizer(
                Collections.emptyList(),
                new StaticGoalAssignmentStrategy(),
                false,
                600000
        );

        // Should throw exception when optimization is disabled
        optimizer.optimizeDistributed(null, null, null);
    }
}
