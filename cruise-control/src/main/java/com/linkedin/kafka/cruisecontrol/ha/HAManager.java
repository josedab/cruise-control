/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.HAConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High Availability Manager for Cruise Control.
 *
 * <p>This manager coordinates leader election and state management to provide
 * active-passive high availability across multiple Cruise Control instances.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Manage leader election using the configured coordination service</li>
 *   <li>Coordinate state synchronization between active and standby instances</li>
 *   <li>Handle leadership transitions (active to standby and vice versa)</li>
 *   <li>Expose metrics for monitoring HA health</li>
 * </ul>
 */
public class HAManager {
  private static final Logger LOG = LoggerFactory.getLogger(HAManager.class);

  private final KafkaCruiseControlConfig _config;
  private final LeaderElectionManager _leaderElectionManager;
  private final ExecutionStateReporter _stateReporter;
  private final ExecutionStateRecovery _stateRecovery;
  private final boolean _enabled;

  // Metrics
  private final Counter _leadershipChanges;
  private final Histogram _failoverDuration;
  private final Counter _leaderElectionErrors;
  private volatile long _lastLeadershipChangeTimeMs;

  /**
   * Create an HA manager.
   *
   * @param config Cruise Control configuration
   * @param metricRegistry registry for HA metrics
   */
  public HAManager(KafkaCruiseControlConfig config, MetricRegistry metricRegistry) {
    this._config = config;
    this._enabled = config.getBoolean(HAConfig.HA_ENABLED_CONFIG);
    this._lastLeadershipChangeTimeMs = 0;

    if (_enabled) {
      LOG.info("Initializing HA manager with coordination service: {}",
               config.getString(HAConfig.HA_COORDINATION_SERVICE_CONFIG));

      // Initialize leader election
      _leaderElectionManager = createLeaderElectionManager(config);

      // Initialize state management
      String bootstrapServers = config.getString("bootstrap.servers");
      String stateTopic = config.getString(HAConfig.HA_EXECUTION_STATE_TOPIC_CONFIG);
      long recoveryTimeoutMs = config.getLong(HAConfig.HA_STATE_RECOVERY_TIMEOUT_MS_CONFIG);

      _stateReporter = new ExecutionStateReporter(bootstrapServers, stateTopic, true);
      _stateRecovery = new ExecutionStateRecovery(bootstrapServers, stateTopic, recoveryTimeoutMs);

      // Initialize metrics
      _leadershipChanges = metricRegistry.counter(
          MetricRegistry.name("kafka.cruisecontrol", "HAManager", "LeadershipChanges"));
      _failoverDuration = metricRegistry.histogram(
          MetricRegistry.name("kafka.cruisecontrol", "HAManager", "FailoverDuration"));
      _leaderElectionErrors = metricRegistry.counter(
          MetricRegistry.name("kafka.cruisecontrol", "HAManager", "LeaderElectionErrors"));

      metricRegistry.register(
          MetricRegistry.name("kafka.cruisecontrol", "HAManager", "IsLeader"),
          (Gauge<Integer>) () -> _leaderElectionManager.isLeader() ? 1 : 0);

      metricRegistry.register(
          MetricRegistry.name("kafka.cruisecontrol", "HAManager", "CoordinationServiceConnected"),
          (Gauge<Integer>) () -> _leaderElectionManager.isCoordinationServiceHealthy() ? 1 : 0);

      LOG.info("HA manager initialized successfully");
    } else {
      LOG.info("HA is disabled");
      _leaderElectionManager = null;
      _stateReporter = null;
      _stateRecovery = null;
      _leadershipChanges = null;
      _failoverDuration = null;
      _leaderElectionErrors = null;
    }
  }

  /**
   * Start the HA manager and begin participating in leader election.
   *
   * @param listener listener for leadership change events
   * @throws Exception if HA manager cannot be started
   */
  public void start(LeadershipListener listener) throws Exception {
    if (!_enabled) {
      LOG.debug("HA is disabled, not starting HA manager");
      return;
    }

    LOG.info("Starting HA manager");

    // Wrap the listener to track metrics
    LeadershipListener wrappedListener = new LeadershipListener() {
      @Override
      public void onLeadershipGained() {
        long startTime = System.currentTimeMillis();
        LOG.info("Leadership gained, transitioning to ACTIVE mode");

        try {
          listener.onLeadershipGained();
          long duration = System.currentTimeMillis() - startTime;
          _failoverDuration.update(duration);
          _leadershipChanges.inc();
          _lastLeadershipChangeTimeMs = System.currentTimeMillis();
          LOG.info("Transition to ACTIVE completed in {}ms", duration);
        } catch (Exception e) {
          LOG.error("Error during leadership gained callback", e);
          _leaderElectionErrors.inc();
          throw e;
        }
      }

      @Override
      public void onLeadershipLost() {
        LOG.info("Leadership lost, transitioning to STANDBY mode");

        try {
          listener.onLeadershipLost();
          _leadershipChanges.inc();
          _lastLeadershipChangeTimeMs = System.currentTimeMillis();
          LOG.info("Transition to STANDBY completed");
        } catch (Exception e) {
          LOG.error("Error during leadership lost callback", e);
          _leaderElectionErrors.inc();
          throw e;
        }
      }
    };

    _leaderElectionManager.registerLeadershipListener(wrappedListener);
    _leaderElectionManager.start();

    LOG.info("HA manager started successfully");
  }

  /**
   * Stop the HA manager and stop participating in leader election.
   */
  public void stop() {
    if (!_enabled) {
      return;
    }

    LOG.info("Stopping HA manager");

    if (_leaderElectionManager != null) {
      _leaderElectionManager.stop();
    }

    if (_stateReporter != null) {
      _stateReporter.close();
    }

    LOG.info("HA manager stopped");
  }

  /**
   * Check if HA is enabled.
   *
   * @return true if HA is enabled
   */
  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Check if this instance is currently the leader.
   *
   * @return true if this is the leader, false if standby or HA disabled
   */
  public boolean isLeader() {
    return !_enabled || (_leaderElectionManager != null && _leaderElectionManager.isLeader());
  }

  /**
   * Get the current leader's instance ID.
   *
   * @return leader instance ID, or null if HA disabled or no leader
   */
  public String getCurrentLeader() {
    if (!_enabled || _leaderElectionManager == null) {
      return null;
    }
    return _leaderElectionManager.getCurrentLeader();
  }

  /**
   * Get this instance's ID.
   *
   * @return this instance's ID, or null if HA disabled
   */
  public String getInstanceId() {
    if (!_enabled || _leaderElectionManager == null) {
      return null;
    }
    return _leaderElectionManager.getInstanceId();
  }

  /**
   * Check if the coordination service is healthy.
   *
   * @return true if connected to coordination service
   */
  public boolean isCoordinationServiceHealthy() {
    if (!_enabled || _leaderElectionManager == null) {
      return true; // If HA disabled, consider it "healthy"
    }
    return _leaderElectionManager.isCoordinationServiceHealthy();
  }

  /**
   * Get the execution state reporter.
   *
   * @return the state reporter, or null if HA disabled
   */
  public ExecutionStateReporter getStateReporter() {
    return _stateReporter;
  }

  /**
   * Get the execution state recovery manager.
   *
   * @return the state recovery manager, or null if HA disabled
   */
  public ExecutionStateRecovery getStateRecovery() {
    return _stateRecovery;
  }

  /**
   * Get the timestamp of the last leadership change.
   *
   * @return timestamp in milliseconds, or 0 if no change has occurred
   */
  public long getLastLeadershipChangeTimeMs() {
    return _lastLeadershipChangeTimeMs;
  }

  /**
   * Voluntarily resign leadership (for maintenance/testing).
   */
  public void resignLeadership() {
    if (_enabled && _leaderElectionManager != null) {
      LOG.info("Voluntarily resigning leadership");
      _leaderElectionManager.resignLeadership();
    }
  }

  /**
   * Create a leader election manager based on the configured coordination service.
   */
  private LeaderElectionManager createLeaderElectionManager(KafkaCruiseControlConfig config) {
    String coordinationService = config.getString(HAConfig.HA_COORDINATION_SERVICE_CONFIG);

    switch (coordinationService.toLowerCase()) {
      case "zookeeper":
        return createZooKeeperLeaderElector(config);

      case "etcd":
        LOG.warn("etcd coordination service is not yet implemented, falling back to ZooKeeper");
        return createZooKeeperLeaderElector(config);

      case "kubernetes":
        LOG.warn("Kubernetes coordination service is not yet implemented, falling back to ZooKeeper");
        return createZooKeeperLeaderElector(config);

      default:
        throw new IllegalArgumentException("Unsupported coordination service: " + coordinationService);
    }
  }

  /**
   * Create a ZooKeeper-based leader elector.
   */
  private LeaderElectionManager createZooKeeperLeaderElector(KafkaCruiseControlConfig config) {
    String zkConnect = config.getString(HAConfig.HA_ZOOKEEPER_CONNECT_CONFIG);
    if (zkConnect == null || zkConnect.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "ZooKeeper connect string is required when HA is enabled with ZooKeeper coordination");
    }

    String electionPath = config.getString(HAConfig.HA_ZOOKEEPER_ELECTION_PATH_CONFIG);
    String instanceId = config.getString(HAConfig.HA_INSTANCE_ID_CONFIG);
    int sessionTimeoutMs = config.getInt(HAConfig.HA_ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG);

    return new ZooKeeperLeaderElector(zkConnect, electionPath, instanceId, sessionTimeoutMs);
  }
}
