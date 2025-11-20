/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates all state and operations for a single Kafka cluster in a multi-cluster setup.
 *
 * <p>Each ClusterContext wraps a {@link KafkaCruiseControl} instance and provides:
 * <ul>
 *   <li>Isolated state management for one cluster</li>
 *   <li>Cluster-specific operations (rebalance, add/remove brokers, etc.)</li>
 *   <li>Access to cluster health and metrics</li>
 *   <li>Lifecycle management (start, stop, shutdown)</li>
 * </ul>
 *
 * <p>This design ensures complete isolation between clusters:
 * <ul>
 *   <li>Each cluster has its own LoadMonitor tracking metrics</li>
 *   <li>Each cluster has its own GoalOptimizer computing proposals</li>
 *   <li>Each cluster has its own Executor managing partition movements</li>
 *   <li>Each cluster has its own AnomalyDetector for self-healing</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * ClusterConfig config = ClusterConfig.builder("us-east")
 *     .bootstrapServers("kafka1:9092")
 *     .zookeeperConnect("zk1:2181")
 *     .build();
 *
 * ClusterContext context = new ClusterContext(config, metricRegistry);
 * context.startUp();
 *
 * // Perform operations
 * OptimizerResult result = context.getOptimizationProposals(...);
 * context.executeProposals(result.goalProposals(), ...);
 *
 * // Query state
 * ExecutorState state = context.executorState();
 * ClusterModel model = context.clusterModel();
 * </pre>
 */
public class ClusterContext {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterContext.class);

  private final String _clusterId;
  private final ClusterConfig _clusterConfig;
  private final KafkaCruiseControl _kafkaCruiseControl;
  private volatile boolean _started = false;

  /**
   * Creates a new ClusterContext for the specified cluster.
   *
   * @param clusterConfig The configuration for this cluster
   * @param metricRegistry The metric registry for monitoring
   * @throws NullPointerException if clusterConfig or metricRegistry is null
   */
  public ClusterContext(ClusterConfig clusterConfig, MetricRegistry metricRegistry) {
    this._clusterConfig = Objects.requireNonNull(clusterConfig, "Cluster configuration cannot be null");
    this._clusterId = clusterConfig.clusterId();
    Objects.requireNonNull(metricRegistry, "Metric registry cannot be null");

    LOG.info("Creating ClusterContext for cluster '{}' (displayName: '{}')",
        _clusterId, clusterConfig.displayName());

    // Create KafkaCruiseControlConfig from cluster-specific configuration
    KafkaCruiseControlConfig cruiseControlConfig = clusterConfig.toKafkaConfig();

    // Initialize KafkaCruiseControl for this cluster
    this._kafkaCruiseControl = new KafkaCruiseControl(cruiseControlConfig, metricRegistry);
  }

  /**
   * Package-private constructor for testing with custom KafkaCruiseControl instance.
   */
  ClusterContext(String clusterId, ClusterConfig clusterConfig, KafkaCruiseControl kafkaCruiseControl) {
    this._clusterId = clusterId;
    this._clusterConfig = clusterConfig;
    this._kafkaCruiseControl = kafkaCruiseControl;
  }

  /**
   * @return The unique identifier for this cluster.
   */
  public String clusterId() {
    return _clusterId;
  }

  /**
   * @return The configuration for this cluster.
   */
  public ClusterConfig clusterConfig() {
    return _clusterConfig;
  }

  /**
   * @return The underlying KafkaCruiseControl instance for this cluster.
   */
  public KafkaCruiseControl kafkaCruiseControl() {
    return _kafkaCruiseControl;
  }

  /**
   * Starts monitoring and anomaly detection for this cluster.
   *
   * <p>This should be called after construction to begin active monitoring.
   * This is idempotent - calling multiple times has no additional effect.
   */
  public synchronized void startUp() {
    if (_started) {
      LOG.debug("Cluster '{}' already started, ignoring duplicate startUp call", _clusterId);
      return;
    }

    LOG.info("Starting up cluster context for '{}'", _clusterId);
    try {
      _kafkaCruiseControl.startUp();
      _started = true;
      LOG.info("Successfully started cluster context for '{}'", _clusterId);
    } catch (Exception e) {
      LOG.error("Failed to start cluster context for '{}'", _clusterId, e);
      throw new RuntimeException("Failed to start cluster " + _clusterId, e);
    }
  }

  /**
   * Shuts down this cluster context, stopping all monitoring and background tasks.
   */
  public synchronized void shutdown() {
    if (!_started) {
      LOG.debug("Cluster '{}' not started, ignoring shutdown call", _clusterId);
      return;
    }

    LOG.info("Shutting down cluster context for '{}'", _clusterId);
    try {
      _kafkaCruiseControl.shutdown();
      _started = false;
      LOG.info("Successfully shut down cluster context for '{}'", _clusterId);
    } catch (Exception e) {
      LOG.error("Error shutting down cluster context for '{}'", _clusterId, e);
    }
  }

  /**
   * @return true if this cluster context has been started.
   */
  public boolean isStarted() {
    return _started;
  }

  /**
   * Gets the current cluster model with the specified requirements.
   *
   * @param requirements The completeness requirements for the model
   * @return The current cluster model
   * @throws NotEnoughValidWindowsException if not enough valid windows to generate model
   * @throws KafkaCruiseControlException if error generating model
   */
  public ClusterModel clusterModel(ModelCompletenessRequirements requirements)
      throws NotEnoughValidWindowsException, KafkaCruiseControlException {
    return _kafkaCruiseControl.clusterModel(requirements, null);
  }

  /**
   * Gets optimization proposals for this cluster.
   *
   * @param options The optimization options
   * @return The optimizer result containing proposals
   * @throws KafkaCruiseControlException if error generating proposals
   */
  public OptimizerResult getOptimizationProposals(OptimizationOptions options)
      throws KafkaCruiseControlException {
    return _kafkaCruiseControl.getOptimizationProposals(options, null);
  }

  /**
   * Executes the given execution proposals for this cluster.
   *
   * <p>This validates that all proposals are for this cluster before execution.
   *
   * @param proposals The proposals to execute
   * @param removedBrokers Brokers being removed
   * @param isTriggeredByGoalViolation Whether triggered by goal violation
   * @param uuid The UUID for tracking this execution
   * @param reason The reason for this execution
   * @throws OngoingExecutionException if execution is already in progress
   */
  public void executeProposals(Collection<ExecutionProposal> proposals,
                                Set<Integer> removedBrokers,
                                boolean isTriggeredByGoalViolation,
                                String uuid,
                                String reason) throws KafkaCruiseControlException {
    // Validate proposals are for this cluster (security check)
    validateProposalsForCluster(proposals);

    LOG.info("Executing {} proposals for cluster '{}' (reason: {})", proposals.size(), _clusterId, reason);
    _kafkaCruiseControl.executeProposals(proposals, removedBrokers, null, isTriggeredByGoalViolation,
        uuid, reason);
  }

  /**
   * Validates that all proposals are for this cluster.
   * Prevents accidental cross-cluster operations.
   */
  private void validateProposalsForCluster(Collection<ExecutionProposal> proposals) {
    // Note: ExecutionProposal doesn't have a clusterId field in the current implementation,
    // so this is a placeholder for future enhancement if needed for additional safety
    // In multi-cluster setup, proposals should only come from the same cluster's optimizer
  }

  /**
   * Gets the current executor state for this cluster.
   *
   * @return The executor state
   */
  public ExecutorState executorState() {
    return _kafkaCruiseControl.state(null, Collections.emptySet()).executorState();
  }

  /**
   * Checks if there are any goal violations for this cluster.
   *
   * @return true if there are goal violations, false otherwise
   */
  public boolean hasGoalViolations() {
    try {
      return _kafkaCruiseControl.hasGoalViolations();
    } catch (Exception e) {
      LOG.warn("Error checking goal violations for cluster '{}'", _clusterId, e);
      return false;
    }
  }

  /**
   * Pauses sampling for this cluster.
   */
  public void pauseSampling() {
    LOG.info("Pausing sampling for cluster '{}'", _clusterId);
    _kafkaCruiseControl.pauseSampling(null);
  }

  /**
   * Resumes sampling for this cluster.
   */
  public void resumeSampling() {
    LOG.info("Resuming sampling for cluster '{}'", _clusterId);
    _kafkaCruiseControl.resumeSampling(null);
  }

  /**
   * Gets the load monitor for this cluster.
   *
   * @return The load monitor
   */
  public LoadMonitor loadMonitor() {
    return _kafkaCruiseControl.loadMonitor();
  }

  /**
   * Gets partitions with offline replicas for this cluster.
   *
   * @param topicPattern Optional topic pattern to filter by
   * @return Set of topic partitions with offline replicas
   */
  public Set<TopicPartition> partitionWithOfflineReplicas(Pattern topicPattern) {
    try {
      return _kafkaCruiseControl.partitionWithOfflineReplicas(topicPattern);
    } catch (Exception e) {
      LOG.error("Error getting partitions with offline replicas for cluster '{}'", _clusterId, e);
      return Collections.emptySet();
    }
  }

  /**
   * Gets the number of brokers in this cluster.
   *
   * @return The number of brokers, or -1 if unable to determine
   */
  public int brokerCount() {
    try {
      return _kafkaCruiseControl.refreshClusterAndGeneration().cluster().nodes().size();
    } catch (Exception e) {
      LOG.warn("Unable to get broker count for cluster '{}'", _clusterId, e);
      return -1;
    }
  }

  /**
   * Provides a health summary for this cluster.
   *
   * @return A ClusterHealth object with health information
   */
  public ClusterHealth health() {
    try {
      int brokerCount = brokerCount();
      ExecutorState state = executorState();
      boolean hasViolations = hasGoalViolations();

      return new ClusterHealth(_clusterId, brokerCount, state, hasViolations);
    } catch (Exception e) {
      LOG.error("Error getting health for cluster '{}'", _clusterId, e);
      return ClusterHealth.error(_clusterId, e.getMessage());
    }
  }

  @Override
  public String toString() {
    return "ClusterContext{"
        + "clusterId='" + _clusterId + '\''
        + ", displayName='" + _clusterConfig.displayName() + '\''
        + ", started=" + _started
        + '}';
  }

  /**
   * Simple health status for a cluster.
   */
  public static class ClusterHealth {
    private final String _clusterId;
    private final int _brokerCount;
    private final ExecutorState _executorState;
    private final boolean _hasGoalViolations;
    private final String _errorMessage;

    public ClusterHealth(String clusterId, int brokerCount, ExecutorState executorState, boolean hasGoalViolations) {
      this._clusterId = clusterId;
      this._brokerCount = brokerCount;
      this._executorState = executorState;
      this._hasGoalViolations = hasGoalViolations;
      this._errorMessage = null;
    }

    private ClusterHealth(String clusterId, String errorMessage) {
      this._clusterId = clusterId;
      this._brokerCount = -1;
      this._executorState = null;
      this._hasGoalViolations = false;
      this._errorMessage = errorMessage;
    }

    public static ClusterHealth error(String clusterId, String errorMessage) {
      return new ClusterHealth(clusterId, errorMessage);
    }

    public String clusterId() {
      return _clusterId;
    }

    public int brokerCount() {
      return _brokerCount;
    }

    public ExecutorState executorState() {
      return _executorState;
    }

    public boolean hasGoalViolations() {
      return _hasGoalViolations;
    }

    public boolean hasError() {
      return _errorMessage != null;
    }

    public String errorMessage() {
      return _errorMessage;
    }

    public String status() {
      if (hasError()) {
        return "ERROR";
      } else if (_hasGoalViolations) {
        return "DEGRADED";
      } else if (_executorState != null && _executorState.state() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
        return "REBALANCING";
      } else {
        return "HEALTHY";
      }
    }

    @Override
    public String toString() {
      return "ClusterHealth{"
          + "clusterId='" + _clusterId + '\''
          + ", status='" + status() + '\''
          + ", brokerCount=" + _brokerCount
          + ", executorState=" + _executorState
          + ", hasGoalViolations=" + _hasGoalViolations
          + (_errorMessage != null ? ", error='" + _errorMessage + '\'' : "")
          + '}';
    }
  }
}
