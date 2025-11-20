/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.simulator.model.Assessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.ImpactAssessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.Recommendation;
import com.linkedin.kafka.cruisecontrol.simulator.model.ResourceUtilization;
import com.linkedin.kafka.cruisecontrol.simulator.model.Severity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes simulation results and generates recommendations.
 * <p>
 * This engine performs capacity analysis, impact assessment, and generates
 * actionable recommendations based on simulation results.
 * </p>
 */
public class AnalysisEngine {
  private static final Logger LOG = LoggerFactory.getLogger(AnalysisEngine.class);

  // Thresholds for capacity assessment
  private static final double HEALTHY_HEADROOM_THRESHOLD = 30.0;
  private static final double ADEQUATE_HEADROOM_THRESHOLD = 15.0;
  private static final double MARGINAL_HEADROOM_THRESHOLD = 5.0;

  // Thresholds for recommendations
  private static final double CRITICAL_UTILIZATION = 95.0;
  private static final double HIGH_UTILIZATION = 90.0;
  private static final double MEDIUM_UTILIZATION = 80.0;
  private static final int LARGE_REBALANCE_THRESHOLD = 10000;
  private static final int MEDIUM_REBALANCE_THRESHOLD = 1000;

  /**
   * Analyzes cluster capacity after scenario modifications.
   *
   * @param model the cluster model to analyze
   * @return capacity analysis results
   */
  public CapacityAnalysis analyzeCapacity(ClusterModel model) {
    LOG.debug("Analyzing capacity for cluster with {} brokers", model.brokers().size());

    Map<Resource, ResourceUtilization> utilization = new HashMap<>();

    for (Resource resource : Resource.cachedValues()) {
      double max = calculateMaxUtilization(model, resource);
      double avg = calculateAverageUtilization(model, resource);
      double p95 = calculateP95Utilization(model, resource);

      // Calculate headroom (percentage of capacity still available)
      double headroom = 100.0 - max;

      // Assess health based on headroom
      Assessment assessment = assessHealth(headroom);

      utilization.put(resource, new ResourceUtilization(
          resource,
          max,
          avg,
          p95,
          headroom,
          assessment
      ));

      LOG.debug("Resource {}: max={}%, avg={}%, p95={}%, headroom={}%, assessment={}",
          resource, String.format("%.2f", max), String.format("%.2f", avg),
          String.format("%.2f", p95), String.format("%.2f", headroom), assessment);
    }

    return new CapacityAnalysis(utilization);
  }

  /**
   * Assesses impact of proposed changes.
   *
   * @param model the cluster model
   * @param optimization the optimization result
   * @return impact assessment
   */
  public ImpactAssessment assessImpact(ClusterModel model, OptimizerResult optimization) {
    LOG.debug("Assessing impact of optimization");

    int partitionMovements = optimization.goalProposals().size();
    long dataToMove = estimateDataToMove(optimization);
    long estimatedDuration = estimateDuration(partitionMovements, dataToMove);
    double cpuImpact = estimateCPUImpact(model, dataToMove, estimatedDuration);
    double networkImpact = estimateNetworkImpact(model, dataToMove, estimatedDuration);
    List<String> risks = identifyRisks(model, optimization);

    return new ImpactAssessment(
        partitionMovements,
        dataToMove,
        estimatedDuration,
        cpuImpact,
        networkImpact,
        risks
    );
  }

  /**
   * Generates actionable recommendations based on analysis.
   *
   * @param model the cluster model
   * @param capacity the capacity analysis
   * @param impact the impact assessment
   * @return list of recommendations
   */
  public List<Recommendation> generateRecommendations(ClusterModel model,
                                                      CapacityAnalysis capacity,
                                                      ImpactAssessment impact) {
    LOG.debug("Generating recommendations");
    List<Recommendation> recommendations = new ArrayList<>();

    // Check CPU capacity
    ResourceUtilization cpu = capacity.cpu();
    if (cpu != null) {
      if (cpu.assessment() == Assessment.INSUFFICIENT) {
        int brokersNeeded = calculateBrokersNeeded(model, Resource.CPU, cpu.max());
        recommendations.add(new Recommendation(
            Severity.CRITICAL,
            "CPU capacity insufficient",
            String.format("Maximum CPU utilization is %.1f%%. Add approximately %d brokers to maintain healthy capacity.",
                cpu.max(), brokersNeeded)
        ));
      } else if (cpu.assessment() == Assessment.MARGINAL) {
        recommendations.add(new Recommendation(
            Severity.HIGH,
            "CPU capacity marginal",
            String.format("CPU headroom is only %.1f%%. Consider adding brokers proactively.", cpu.headroom())
        ));
      }
    }

    // Check disk capacity
    ResourceUtilization disk = capacity.disk();
    if (disk != null) {
      if (disk.max() > CRITICAL_UTILIZATION) {
        recommendations.add(new Recommendation(
            Severity.CRITICAL,
            "Disk capacity critical",
            String.format("Maximum disk utilization is %.1f%%. Immediate action required: add brokers or increase disk capacity.",
                disk.max())
        ));
      } else if (disk.max() > HIGH_UTILIZATION) {
        recommendations.add(new Recommendation(
            Severity.HIGH,
            "Disk capacity high",
            String.format("Disk utilization is %.1f%%. Consider increasing disk size or adding brokers.", disk.max())
        ));
      }
    }

    // Check rebalance impact
    if (impact.partitionMovements() > LARGE_REBALANCE_THRESHOLD) {
      recommendations.add(new Recommendation(
          Severity.MEDIUM,
          "Large rebalance required",
          String.format("%d partitions need to be moved (%.2f GB). Schedule during low traffic period and monitor cluster closely.",
              impact.partitionMovements(), impact.dataToMoveBytes() / (1024.0 * 1024.0 * 1024.0))
      ));
    } else if (impact.partitionMovements() > MEDIUM_REBALANCE_THRESHOLD) {
      recommendations.add(new Recommendation(
          Severity.LOW,
          "Moderate rebalance required",
          String.format("%d partitions need to be moved. Expected duration: %d minutes.",
              impact.partitionMovements(), impact.estimatedDurationMs() / 60000)
      ));
    }

    // Check for risks
    if (!impact.risks().isEmpty()) {
      for (String risk : impact.risks()) {
        recommendations.add(new Recommendation(
            Severity.HIGH,
            "Risk identified",
            risk
        ));
      }
    }

    // Check network impact
    if (impact.networkImpact() > 50.0) {
      recommendations.add(new Recommendation(
          Severity.MEDIUM,
          "High network impact",
          String.format("Rebalancing will increase network utilization by %.1f%%. Monitor network capacity during execution.",
              impact.networkImpact())
      ));
    }

    // Add positive recommendation if everything looks good
    if (recommendations.isEmpty()) {
      recommendations.add(new Recommendation(
          Severity.INFO,
          "Cluster healthy",
          "Cluster can handle the proposed scenario with adequate capacity and low risk."
      ));
    }

    LOG.debug("Generated {} recommendations", recommendations.size());
    return recommendations;
  }

  /**
   * Calculates maximum utilization for a resource across all brokers.
   */
  private double calculateMaxUtilization(ClusterModel model, Resource resource) {
    double max = 0.0;
    for (Broker broker : model.aliveBrokers()) {
      double utilization = broker.load().expectedUtilizationFor(resource);
      if (utilization > max) {
        max = utilization;
      }
    }
    return max * 100.0; // Convert to percentage
  }

  /**
   * Calculates average utilization for a resource across all brokers.
   */
  private double calculateAverageUtilization(ClusterModel model, Resource resource) {
    if (model.aliveBrokers().isEmpty()) {
      return 0.0;
    }

    double sum = 0.0;
    for (Broker broker : model.aliveBrokers()) {
      sum += broker.load().expectedUtilizationFor(resource);
    }
    return (sum / model.aliveBrokers().size()) * 100.0; // Convert to percentage
  }

  /**
   * Calculates 95th percentile utilization for a resource.
   * Simplified implementation - returns a value between max and average.
   */
  private double calculateP95Utilization(ClusterModel model, Resource resource) {
    // Simplified: return value between max and average
    double max = calculateMaxUtilization(model, resource);
    double avg = calculateAverageUtilization(model, resource);
    return avg + (max - avg) * 0.8; // 80% of the way from avg to max
  }

  /**
   * Assesses health based on headroom percentage.
   */
  private Assessment assessHealth(double headroom) {
    if (headroom > HEALTHY_HEADROOM_THRESHOLD) {
      return Assessment.HEALTHY;
    } else if (headroom > ADEQUATE_HEADROOM_THRESHOLD) {
      return Assessment.ADEQUATE;
    } else if (headroom > MARGINAL_HEADROOM_THRESHOLD) {
      return Assessment.MARGINAL;
    } else {
      return Assessment.INSUFFICIENT;
    }
  }

  /**
   * Estimates the amount of data to move based on optimization result.
   */
  private long estimateDataToMove(OptimizerResult optimization) {
    // Simplified: assume average partition size of 100MB
    // In a real implementation, we would calculate actual sizes
    long avgPartitionSize = 100L * 1024 * 1024; // 100 MB
    return optimization.goalProposals().size() * avgPartitionSize;
  }

  /**
   * Estimates duration of rebalancing operation.
   */
  private long estimateDuration(int partitionMovements, long dataToMove) {
    // Simplified estimation: assume 50 MB/s throughput per partition
    // and some overhead
    if (partitionMovements == 0) {
      return 0;
    }

    long throughputBytesPerSec = 50L * 1024 * 1024; // 50 MB/s
    long durationSeconds = dataToMove / throughputBytesPerSec;

    // Add 20% overhead
    durationSeconds = (long) (durationSeconds * 1.2);

    return durationSeconds * 1000; // Convert to milliseconds
  }

  /**
   * Estimates CPU impact during rebalancing.
   */
  private double estimateCPUImpact(ClusterModel model, long dataToMove, long durationMs) {
    // Simplified: estimate based on data rate and cluster size
    if (durationMs == 0 || model.aliveBrokers().isEmpty()) {
      return 0.0;
    }

    // Assume rebalancing adds 10-15% CPU load
    return 12.0;
  }

  /**
   * Estimates network impact during rebalancing.
   */
  private double estimateNetworkImpact(ClusterModel model, long dataToMove, long durationMs) {
    // Simplified: estimate based on data rate
    if (durationMs == 0) {
      return 0.0;
    }

    // Assume network impact is proportional to data volume
    double dataGB = dataToMove / (1024.0 * 1024.0 * 1024.0);
    return Math.min(dataGB / 10.0, 50.0); // Cap at 50%
  }

  /**
   * Identifies potential risks in the optimization.
   */
  private List<String> identifyRisks(ClusterModel model, OptimizerResult optimization) {
    List<String> risks = new ArrayList<>();

    // Check if cluster is too small
    if (model.aliveBrokers().size() < 3) {
      risks.add("Cluster has fewer than 3 alive brokers - limited fault tolerance");
    }

    // Check if many brokers are dead
    if (!model.deadBrokers().isEmpty()) {
      risks.add(String.format("%d brokers are dead - cluster is operating in degraded mode",
          model.deadBrokers().size()));
    }

    return risks;
  }

  /**
   * Calculates number of brokers needed to maintain healthy capacity.
   */
  private int calculateBrokersNeeded(ClusterModel model, Resource resource, double currentMaxUtilization) {
    // Simplified calculation: determine how many brokers to add to bring max utilization to target
    double targetUtilization = 70.0; // Target 70% max utilization for healthy headroom

    if (currentMaxUtilization <= targetUtilization) {
      return 0;
    }

    int currentBrokers = model.aliveBrokers().size();
    // Calculate new total brokers needed
    double brokersNeeded = (currentMaxUtilization / targetUtilization) * currentBrokers;
    int additionalBrokers = (int) Math.ceil(brokersNeeded - currentBrokers);

    return Math.max(1, additionalBrokers); // At least 1 broker
  }
}
