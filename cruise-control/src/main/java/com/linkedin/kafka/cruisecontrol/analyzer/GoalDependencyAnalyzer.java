/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes goal dependencies and groups goals into parallel execution sets.
 * <p>
 *   Goals can run in parallel if they:
 *   <ol>
 *     <li>Modify disjoint sets of resources (no resource conflicts)</li>
 *     <li>Have the same dependency requirements (all depend on the same previously executed goals)</li>
 *   </ol>
 * </p>
 * <p>
 *   Goals are processed in their original priority order. When a goal is encountered, the analyzer
 *   attempts to add it to the current parallel group. If it conflicts with any goal in the current
 *   group, a new group is started.
 * </p>
 */
public class GoalDependencyAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(GoalDependencyAnalyzer.class);

  /**
   * Builds execution groups from a list of goals in priority order.
   * <p>
   *   Each execution group contains goals that can be executed in parallel. Groups must be
   *   executed sequentially, in the order returned.
   * </p>
   *
   * @param goals Goals in priority order (highest priority first)
   * @return List of execution groups, where each group is a set of goals that can run in parallel.
   *         Groups are ordered and must be executed sequentially.
   */
  public List<Set<Goal>> buildExecutionGroups(List<Goal> goals) {
    if (goals == null || goals.isEmpty()) {
      return Collections.emptyList();
    }

    List<Set<Goal>> groups = new ArrayList<>();
    Set<Goal> processed = new HashSet<>();

    for (Goal goal : goals) {
      if (processed.contains(goal)) {
        continue;
      }

      // Try to add to existing group, or create new group
      Set<Goal> parallelGroup = findOrCreateGroup(goal, goals, processed, groups);

      if (parallelGroup != null && !parallelGroup.isEmpty()) {
        if (!groups.contains(parallelGroup)) {
          groups.add(parallelGroup);
        }
        processed.addAll(parallelGroup);

        if (parallelGroup.size() > 1) {
          LOG.info("Parallel execution group: {} goals can run concurrently", parallelGroup.size());
          for (Goal g : parallelGroup) {
            LOG.debug("  - {}", g.name());
          }
        }
      }
    }

    LOG.info("Built {} execution groups from {} goals ({} can run in parallel)",
             groups.size(), goals.size(),
             groups.stream().mapToInt(Set::size).sum() - groups.size());

    return groups;
  }

  /**
   * Finds an existing group that this goal can join, or creates a new group.
   */
  private Set<Goal> findOrCreateGroup(Goal goal, List<Goal> allGoals, Set<Goal> processed,
                                      List<Set<Goal>> existingGroups) {
    // Check if this goal can be added to the last (current) group
    if (!existingGroups.isEmpty()) {
      Set<Goal> currentGroup = existingGroups.get(existingGroups.size() - 1);
      if (canJoinGroup(goal, currentGroup, allGoals, processed)) {
        currentGroup.add(goal);
        return currentGroup;
      }
    }

    // Create a new group with this goal as the first member
    Set<Goal> newGroup = new LinkedHashSet<>();
    newGroup.add(goal);

    // Try to add subsequent goals from the priority list to this group
    for (Goal candidate : allGoals) {
      if (processed.contains(candidate) || candidate.equals(goal)) {
        continue;
      }

      if (canJoinGroup(candidate, newGroup, allGoals, processed)) {
        newGroup.add(candidate);
      }
    }

    return newGroup;
  }

  /**
   * Determines if a goal can join an existing parallel execution group.
   * <p>
   *   A goal can join a group if:
   *   <ol>
   *     <li>It doesn't conflict with any goal already in the group (disjoint resources)</li>
   *     <li>All goals in the group have compatible dependencies</li>
   *   </ol>
   * </p>
   */
  private boolean canJoinGroup(Goal candidate, Set<Goal> group, List<Goal> allGoals, Set<Goal> processed) {
    for (Goal existingGoal : group) {
      if (!canRunInParallel(candidate, existingGoal, allGoals, processed)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines if two goals can run in parallel.
   * <p>
   *   Goals can run in parallel if they modify disjoint sets of resources.
   *   Special handling for goals with empty resource sets (conservative: assume conflicts).
   * </p>
   */
  private boolean canRunInParallel(Goal g1, Goal g2, List<Goal> allGoals, Set<Goal> processed) {
    Set<Resource> resources1 = g1.modifiedResources();
    Set<Resource> resources2 = g2.modifiedResources();

    // Conservative: if either goal has empty resource set (unknown/all resources),
    // they cannot run in parallel
    if (resources1.isEmpty() || resources2.isEmpty()) {
      LOG.debug("Goals {} and {} cannot run in parallel: unknown resource modifications",
                g1.name(), g2.name());
      return false;
    }

    // Check for resource conflicts
    if (!Collections.disjoint(resources1, resources2)) {
      LOG.debug("Goals {} and {} cannot run in parallel: conflicting resources {}",
                g1.name(), g2.name(), getConflictingResources(resources1, resources2));
      return false;
    }

    // Check if goals have compatible dependencies (same set of processed goals)
    // This ensures both goals can start at the same point in the execution sequence
    if (!haveSameDependencies(g1, g2, allGoals, processed)) {
      LOG.debug("Goals {} and {} cannot run in parallel: different dependencies",
                g1.name(), g2.name());
      return false;
    }

    return true;
  }

  /**
   * Checks if two goals have the same dependencies.
   * <p>
   *   For now, we use a simple heuristic: goals can run together if they appear
   *   consecutively in the priority list (or very close together), meaning they
   *   share the same set of prerequisite goals.
   * </p>
   * <p>
   *   Future enhancement: explicit dependency declaration in goals.
   * </p>
   */
  private boolean haveSameDependencies(Goal g1, Goal g2, List<Goal> allGoals, Set<Goal> processed) {
    // Simple heuristic: if both goals come after the same set of processed goals,
    // they have compatible dependencies

    int index1 = allGoals.indexOf(g1);
    int index2 = allGoals.indexOf(g2);

    if (index1 < 0 || index2 < 0) {
      return false;
    }

    // Goals must be close in priority (within a reasonable window)
    // This prevents goals from very different parts of the execution sequence
    // from being grouped together
    int maxGap = 5; // Configurable threshold
    if (Math.abs(index1 - index2) > maxGap) {
      return false;
    }

    // Both goals must come after all currently processed goals
    int minIndex = Math.min(index1, index2);
    for (Goal processedGoal : processed) {
      int processedIndex = allGoals.indexOf(processedGoal);
      if (processedIndex >= minIndex) {
        // A processed goal appears after one of these goals in priority order
        // This violates dependency ordering
        return false;
      }
    }

    return true;
  }

  /**
   * Returns the set of resources that conflict between two resource sets.
   */
  private Set<Resource> getConflictingResources(Set<Resource> resources1, Set<Resource> resources2) {
    Set<Resource> conflicts = new HashSet<>(resources1);
    conflicts.retainAll(resources2);
    return conflicts;
  }

  /**
   * Returns statistics about parallelization potential for a goal list.
   * Useful for logging and monitoring.
   */
  public ParallelizationStats getParallelizationStats(List<Goal> goals) {
    List<Set<Goal>> groups = buildExecutionGroups(goals);

    int totalGoals = goals.size();
    int sequentialGroups = groups.size();
    int parallelGoals = groups.stream().mapToInt(Set::size).sum() - groups.size();
    int maxParallelism = groups.stream().mapToInt(Set::size).max().orElse(0);

    return new ParallelizationStats(totalGoals, sequentialGroups, parallelGoals, maxParallelism);
  }

  /**
   * Statistics about goal parallelization potential.
   */
  public static class ParallelizationStats {
    private final int totalGoals;
    private final int sequentialGroups;
    private final int parallelGoals;
    private final int maxParallelism;

    public ParallelizationStats(int totalGoals, int sequentialGroups, int parallelGoals, int maxParallelism) {
      this.totalGoals = totalGoals;
      this.sequentialGroups = sequentialGroups;
      this.parallelGoals = parallelGoals;
      this.maxParallelism = maxParallelism;
    }

    public int getTotalGoals() {
      return totalGoals;
    }

    public int getSequentialGroups() {
      return sequentialGroups;
    }

    public int getParallelGoals() {
      return parallelGoals;
    }

    public int getMaxParallelism() {
      return maxParallelism;
    }

    public double getParallelizationRatio() {
      return totalGoals > 0 ? (double) parallelGoals / totalGoals : 0.0;
    }

    @Override
    public String toString() {
      return String.format("ParallelizationStats{totalGoals=%d, sequentialGroups=%d, parallelGoals=%d, " +
                          "maxParallelism=%d, parallelizationRatio=%.2f%%}",
                          totalGoals, sequentialGroups, parallelGoals, maxParallelism,
                          getParallelizationRatio() * 100);
    }
  }
}
