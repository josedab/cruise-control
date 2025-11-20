/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.Set;

/**
 * Result object containing the optimized cluster model and metadata from worker execution.
 */
public class WorkerOptimizationResult {
    private final ClusterModel _optimizedModel;
    private final Set<Goal> _violatedGoals;
    private final long _optimizationTimeMs;

    public WorkerOptimizationResult(ClusterModel optimizedModel, Set<Goal> violatedGoals, long optimizationTimeMs) {
        _optimizedModel = optimizedModel;
        _violatedGoals = violatedGoals;
        _optimizationTimeMs = optimizationTimeMs;
    }

    public ClusterModel getOptimizedModel() {
        return _optimizedModel;
    }

    public Set<Goal> getViolatedGoals() {
        return _violatedGoals;
    }

    public long getOptimizationTimeMs() {
        return _optimizationTimeMs;
    }
}
