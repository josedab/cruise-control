/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper-based implementation of leader election using Apache Curator framework.
 *
 * <p>This implementation uses Curator's {@link LeaderSelector} recipe which provides
 * a robust leader election mechanism with automatic failover. The election is based
 * on ephemeral sequential nodes in ZooKeeper, ensuring that when a leader fails,
 * the next instance in line automatically takes over.
 *
 * <p>Configuration properties used:
 * <ul>
 *   <li>cruise.control.ha.zookeeper.connect - ZooKeeper connection string</li>
 *   <li>cruise.control.ha.zookeeper.session.timeout.ms - Session timeout</li>
 *   <li>cruise.control.ha.election.path - ZooKeeper path for election</li>
 *   <li>cruise.control.ha.instance.id - Unique instance identifier</li>
 * </ul>
 */
public class ZooKeeperLeaderElector implements LeaderElectionManager {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElector.class);
  private static final int DEFAULT_BASE_SLEEP_TIME_MS = 1000;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;
  private static final String DEFAULT_ELECTION_PATH = "/cruise-control/leader";

  private final String _zookeeperConnect;
  private final String _electionPath;
  private final String _instanceId;
  private final int _sessionTimeoutMs;
  private final AtomicBoolean _isLeader;

  private CuratorFramework _curatorClient;
  private LeaderSelector _leaderSelector;
  private LeadershipListener _listener;
  private volatile boolean _started;

  /**
   * Create a ZooKeeper leader elector.
   *
   * @param zookeeperConnect ZooKeeper connection string (e.g., "zk1:2181,zk2:2181")
   * @param electionPath ZooKeeper path for leader election
   * @param instanceId unique identifier for this instance
   * @param sessionTimeoutMs ZooKeeper session timeout in milliseconds
   */
  public ZooKeeperLeaderElector(String zookeeperConnect, String electionPath, String instanceId, int sessionTimeoutMs) {
    if (zookeeperConnect == null || zookeeperConnect.trim().isEmpty()) {
      throw new IllegalArgumentException("ZooKeeper connect string cannot be null or empty");
    }
    this._zookeeperConnect = zookeeperConnect;
    this._electionPath = electionPath != null ? electionPath : DEFAULT_ELECTION_PATH;
    this._instanceId = instanceId != null ? instanceId : getDefaultInstanceId();
    this._sessionTimeoutMs = sessionTimeoutMs > 0 ? sessionTimeoutMs : DEFAULT_SESSION_TIMEOUT_MS;
    this._isLeader = new AtomicBoolean(false);
    this._started = false;

    LOG.info("Created ZooKeeperLeaderElector with instanceId={}, zkConnect={}, electionPath={}",
             _instanceId, _zookeeperConnect, _electionPath);
  }

  /**
   * Get default instance ID based on hostname.
   */
  private static String getDefaultInstanceId() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (IOException e) {
      LOG.warn("Failed to get hostname, using timestamp-based ID", e);
      return "cruise-control-" + System.currentTimeMillis();
    }
  }

  @Override
  public synchronized void start() throws Exception {
    if (_started) {
      LOG.warn("Leader election already started for instance {}", _instanceId);
      return;
    }

    LOG.info("Starting leader election for instance {} with ZooKeeper at {}", _instanceId, _zookeeperConnect);

    // Create Curator client with retry policy
    _curatorClient = CuratorFrameworkFactory.newClient(
        _zookeeperConnect,
        _sessionTimeoutMs,
        _sessionTimeoutMs, // connection timeout same as session timeout
        new ExponentialBackoffRetry(DEFAULT_BASE_SLEEP_TIME_MS, DEFAULT_MAX_RETRIES)
    );

    _curatorClient.start();

    // Wait for connection with timeout
    if (!_curatorClient.blockUntilConnected(30, TimeUnit.SECONDS)) {
      throw new IOException("Failed to connect to ZooKeeper within 30 seconds");
    }

    LOG.info("Connected to ZooKeeper successfully");

    // Create leader selector
    _leaderSelector = new LeaderSelector(_curatorClient, _electionPath, new LeaderSelectorListenerAdapter() {
      @Override
      public void takeLeadership(CuratorFramework client) throws Exception {
        // This method is called when this instance becomes the leader
        LOG.info("Instance {} acquired leadership", _instanceId);
        _isLeader.set(true);

        if (_listener != null) {
          try {
            _listener.onLeadershipGained();
          } catch (Exception e) {
            LOG.error("Error in leadership gained callback", e);
          }
        }

        // Keep leadership until interrupted or connection lost
        // The LeaderSelector will call stateChanged() when connection is lost
        try {
          Thread.currentThread().join();
        } catch (InterruptedException e) {
          LOG.info("Leadership interrupted for instance {}", _instanceId);
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // Handle connection state changes
        switch (newState) {
          case SUSPENDED:
            LOG.warn("ZooKeeper connection SUSPENDED for instance {}", _instanceId);
            break;
          case RECONNECTED:
            LOG.info("ZooKeeper connection RECONNECTED for instance {}", _instanceId);
            break;
          case LOST:
            LOG.error("ZooKeeper connection LOST for instance {}", _instanceId);
            if (_isLeader.compareAndSet(true, false)) {
              LOG.info("Instance {} lost leadership due to connection loss", _instanceId);
              if (_listener != null) {
                try {
                  _listener.onLeadershipLost();
                } catch (Exception e) {
                  LOG.error("Error in leadership lost callback", e);
                }
              }
            }
            break;
          default:
            LOG.debug("ZooKeeper connection state changed to {} for instance {}", newState, _instanceId);
        }
      }
    });

    // Set instance ID for identification
    _leaderSelector.setId(_instanceId);

    // Automatically requeue for leadership when leadership is relinquished
    _leaderSelector.autoRequeue();

    // Start the leader selection process
    _leaderSelector.start();

    _started = true;
    LOG.info("Leader election started for instance {}", _instanceId);
  }

  @Override
  public void registerLeadershipListener(LeadershipListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("Leadership listener cannot be null");
    }
    this._listener = listener;
    LOG.debug("Registered leadership listener for instance {}", _instanceId);
  }

  @Override
  public boolean isLeader() {
    return _isLeader.get();
  }

  @Override
  public String getCurrentLeader() {
    if (_leaderSelector == null || !_started) {
      return null;
    }

    try {
      Participant leader = _leaderSelector.getLeader();
      return leader != null ? leader.getId() : null;
    } catch (Exception e) {
      LOG.error("Failed to get current leader", e);
      return null;
    }
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public void resignLeadership() {
    if (!_isLeader.get()) {
      LOG.debug("Instance {} is not the leader, nothing to resign", _instanceId);
      return;
    }

    LOG.info("Instance {} voluntarily resigning leadership", _instanceId);
    _isLeader.set(false);

    if (_listener != null) {
      try {
        _listener.onLeadershipLost();
      } catch (Exception e) {
        LOG.error("Error in leadership lost callback during resignation", e);
      }
    }

    // Interrupt the leadership thread to trigger reelection
    _leaderSelector.interruptLeadership();
  }

  @Override
  public synchronized void stop() {
    if (!_started) {
      LOG.debug("Leader election not started for instance {}, nothing to stop", _instanceId);
      return;
    }

    LOG.info("Stopping leader election for instance {}", _instanceId);

    if (_isLeader.get()) {
      resignLeadership();
    }

    if (_leaderSelector != null) {
      try {
        _leaderSelector.close();
      } catch (Exception e) {
        LOG.error("Error closing leader selector", e);
      }
    }

    if (_curatorClient != null) {
      _curatorClient.close();
    }

    _started = false;
    LOG.info("Leader election stopped for instance {}", _instanceId);
  }

  @Override
  public boolean isCoordinationServiceHealthy() {
    return _curatorClient != null && _curatorClient.getZookeeperClient().isConnected();
  }
}
