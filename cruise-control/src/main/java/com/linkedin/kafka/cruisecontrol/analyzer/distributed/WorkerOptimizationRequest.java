/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.List;
import java.util.Set;

/**
 * Request object for worker optimization containing the cluster model and goals to execute.
 */
public class WorkerOptimizationRequest {
    private final ClusterModel _model;
    private final List<Goal> _goals;
    private final Set<Goal> _optimizedGoals;

    public WorkerOptimizationRequest(ClusterModel model, List<Goal> goals, Set<Goal> optimizedGoals) {
        _model = model;
        _goals = goals;
        _optimizedGoals = optimizedGoals;
    }

    public ClusterModel getModel() {
        return _model;
    }

    public List<Goal> getGoals() {
        return _goals;
    }

    public Set<Goal> getOptimizedGoals() {
        return _optimizedGoals;
    }
}
