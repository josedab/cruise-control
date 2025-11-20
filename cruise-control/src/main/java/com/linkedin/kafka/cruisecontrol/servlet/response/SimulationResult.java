/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.simulator.model.CapacityAnalysis;
import com.linkedin.kafka.cruisecontrol.simulator.model.ClusterStats;
import com.linkedin.kafka.cruisecontrol.simulator.model.ImpactAssessment;
import com.linkedin.kafka.cruisecontrol.simulator.model.Recommendation;
import com.linkedin.kafka.cruisecontrol.simulator.model.ResourceUtilization;
import com.linkedin.kafka.cruisecontrol.simulator.model.SimulationReport;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Response for simulation endpoint.
 * <p>
 * This class formats simulation results for JSON or plain text output.
 * </p>
 */
@JsonResponseClass
public class SimulationResult extends AbstractCruiseControlResponse {
  @JsonResponseField
  private static final String SCENARIO_NAME = "scenarioName";
  @JsonResponseField
  private static final String CLUSTER_STATS = "clusterStats";
  @JsonResponseField
  private static final String CAPACITY_ANALYSIS = "capacityAnalysis";
  @JsonResponseField
  private static final String IMPACT_ASSESSMENT = "impactAssessment";
  @JsonResponseField
  private static final String RECOMMENDATIONS = "recommendations";

  private final SimulationReport _report;

  public SimulationResult(SimulationReport report, KafkaCruiseControlConfig config) {
    super(config);
    _report = report;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant information for text response
    _cachedResponse = formatTextResponse();
  }

  /**
   * Formats the simulation report as a JSON string.
   *
   * @return JSON representation of the simulation results
   */
  @Override
  protected String getPlaintext() {
    return _cachedResponse != null ? _cachedResponse : formatTextResponse();
  }

  /**
   * Formats the simulation report as JSON.
   *
   * @return JSON map of simulation results
   */
  @Override
  protected Map<String, Object> getJsonStructure() {
    Map<String, Object> result = new HashMap<>();

    result.put(SCENARIO_NAME, _report.scenarioName());
    result.put(CLUSTER_STATS, formatClusterStats(_report.clusterStats()));
    result.put(CAPACITY_ANALYSIS, formatCapacityAnalysis(_report.capacityAnalysis()));
    result.put(IMPACT_ASSESSMENT, formatImpactAssessment(_report.impactAssessment()));
    result.put(RECOMMENDATIONS, formatRecommendations(_report.recommendations()));

    return result;
  }

  private String formatTextResponse() {
    StringBuilder sb = new StringBuilder();

    sb.append("Simulation Results: ").append(_report.scenarioName()).append("\n");
    sb.append("=".repeat(80)).append("\n\n");

    // Cluster stats
    ClusterStats stats = _report.clusterStats();
    sb.append("Cluster Configuration:\n");
    sb.append("  Brokers: ").append(stats.brokers()).append("\n");
    sb.append("  Partitions: ").append(stats.partitions()).append("\n");
    sb.append("  Replicas: ").append(stats.replicas()).append("\n");
    sb.append("  Total Data: ").append(formatBytes(stats.totalDataBytes())).append("\n\n");

    // Capacity analysis
    sb.append("Capacity Analysis:\n");
    CapacityAnalysis capacity = _report.capacityAnalysis();
    for (ResourceUtilization util : capacity.utilization().values()) {
      sb.append(String.format("  %s: max=%.1f%%, avg=%.1f%%, headroom=%.1f%%, status=%s\n",
          util.resource(), util.max(), util.average(), util.headroom(), util.assessment()));
    }
    sb.append("\n");

    // Impact assessment
    ImpactAssessment impact = _report.impactAssessment();
    sb.append("Impact Assessment:\n");
    sb.append("  Partition movements: ").append(impact.partitionMovements()).append("\n");
    sb.append("  Data to move: ").append(formatBytes(impact.dataToMoveBytes())).append("\n");
    sb.append("  Estimated duration: ").append(formatDuration(impact.estimatedDurationMs())).append("\n");
    sb.append("  CPU impact: +").append(String.format("%.1f", impact.cpuImpact())).append("%\n");
    sb.append("  Network impact: +").append(String.format("%.1f", impact.networkImpact())).append("%\n\n");

    // Recommendations
    sb.append("Recommendations:\n");
    List<Recommendation> recs = _report.recommendations();
    if (recs.isEmpty()) {
      sb.append("  None\n");
    } else {
      for (Recommendation rec : recs) {
        sb.append(String.format("  [%s] %s: %s\n", rec.severity(), rec.title(), rec.description()));
      }
    }

    return sb.toString();
  }

  private Map<String, Object> formatClusterStats(ClusterStats stats) {
    Map<String, Object> map = new HashMap<>();
    map.put("brokers", stats.brokers());
    map.put("partitions", stats.partitions());
    map.put("replicas", stats.replicas());
    map.put("totalDataBytes", stats.totalDataBytes());
    return map;
  }

  private Map<String, Object> formatCapacityAnalysis(CapacityAnalysis capacity) {
    Map<String, Object> map = new HashMap<>();
    for (Map.Entry<com.linkedin.kafka.cruisecontrol.common.Resource, ResourceUtilization> entry
        : capacity.utilization().entrySet()) {
      Map<String, Object> resourceMap = new HashMap<>();
      ResourceUtilization util = entry.getValue();
      resourceMap.put("max", util.max());
      resourceMap.put("average", util.average());
      resourceMap.put("p95", util.p95());
      resourceMap.put("headroom", util.headroom());
      resourceMap.put("assessment", util.assessment().toString());
      map.put(entry.getKey().toString(), resourceMap);
    }
    return map;
  }

  private Map<String, Object> formatImpactAssessment(ImpactAssessment impact) {
    Map<String, Object> map = new HashMap<>();
    map.put("partitionMovements", impact.partitionMovements());
    map.put("dataToMoveBytes", impact.dataToMoveBytes());
    map.put("estimatedDurationMs", impact.estimatedDurationMs());
    map.put("cpuImpact", impact.cpuImpact());
    map.put("networkImpact", impact.networkImpact());
    map.put("risks", impact.risks());
    return map;
  }

  private List<Map<String, Object>> formatRecommendations(List<Recommendation> recommendations) {
    return recommendations.stream().map(rec -> {
      Map<String, Object> map = new HashMap<>();
      map.put("severity", rec.severity().toString());
      map.put("title", rec.title());
      map.put("description", rec.description());
      return map;
    }).collect(Collectors.toList());
  }

  private String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.2f KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.2f MB", bytes / (1024.0 * 1024));
    } else {
      return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
  }

  private String formatDuration(long ms) {
    long seconds = ms / 1000;
    long minutes = seconds / 60;
    long hours = minutes / 60;

    if (hours > 0) {
      return String.format("%dh %dm", hours, minutes % 60);
    } else if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds % 60);
    } else {
      return String.format("%ds", seconds);
    }
  }
}
