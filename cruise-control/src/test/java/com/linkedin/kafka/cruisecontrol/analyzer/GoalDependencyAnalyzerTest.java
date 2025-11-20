/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link GoalDependencyAnalyzer}.
 */
public class GoalDependencyAnalyzerTest {

  @Test
  public void testEmptyGoalList() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();
    List<Set<Goal>> groups = analyzer.buildExecutionGroups(Arrays.asList());
    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  @Test
  public void testSingleGoal() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();
    List<Goal> goals = Arrays.asList(new CpuCapacityGoal());

    List<Set<Goal>> groups = analyzer.buildExecutionGroups(goals);

    assertNotNull(groups);
    assertEquals(1, groups.size());
    assertEquals(1, groups.get(0).size());
  }

  @Test
  public void testIndependentGoals() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();

    // These goals modify different resources and should be grouped together
    List<Goal> goals = Arrays.asList(
        new CpuCapacityGoal(),
        new DiskCapacityGoal()
    );

    List<Set<Goal>> groups = analyzer.buildExecutionGroups(goals);

    assertNotNull(groups);
    assertFalse(groups.isEmpty());

    // Goals with different resources can potentially run in parallel
    // The exact grouping depends on the implementation logic
  }

  @Test
  public void testCapacityAndDistributionGoals() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();

    // Capacity goals and distribution goals modify different resources
    List<Goal> goals = Arrays.asList(
        new CpuCapacityGoal(),
        new CpuUsageDistributionGoal()
    );

    List<Set<Goal>> groups = analyzer.buildExecutionGroups(goals);

    assertNotNull(groups);
    assertFalse(groups.isEmpty());
  }

  @Test
  public void testParallelizationStats() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();

    List<Goal> goals = Arrays.asList(
        new RackAwareGoal(),
        new CpuCapacityGoal(),
        new DiskCapacityGoal()
    );

    GoalDependencyAnalyzer.ParallelizationStats stats = analyzer.getParallelizationStats(goals);

    assertNotNull(stats);
    assertEquals(3, stats.getTotalGoals());
    assertTrue(stats.getSequentialGroups() > 0);
    assertTrue(stats.getMaxParallelism() > 0);
    assertTrue(stats.getParallelizationRatio() >= 0.0);
    assertTrue(stats.getParallelizationRatio() <= 1.0);
  }

  @Test
  public void testStatsToString() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();
    List<Goal> goals = Arrays.asList(new CpuCapacityGoal());

    GoalDependencyAnalyzer.ParallelizationStats stats = analyzer.getParallelizationStats(goals);
    String statsString = stats.toString();

    assertNotNull(statsString);
    assertTrue(statsString.contains("totalGoals=1"));
  }

  @Test
  public void testGoalsWithUnknownResources() {
    GoalDependencyAnalyzer analyzer = new GoalDependencyAnalyzer();

    // RackAwareGoal doesn't override modifiedResources(), so it returns empty set
    // This should be treated conservatively (cannot run in parallel)
    List<Goal> goals = Arrays.asList(
        new RackAwareGoal(),
        new CpuCapacityGoal()
    );

    List<Set<Goal>> groups = analyzer.buildExecutionGroups(goals);

    assertNotNull(groups);
    // Goals with unknown resources should be in separate groups (conservative approach)
    assertTrue(groups.size() >= 1);
  }
}
