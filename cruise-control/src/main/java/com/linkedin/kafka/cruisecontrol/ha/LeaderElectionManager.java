/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

/**
 * Interface for managing leader election in a high-availability Cruise Control cluster.
 *
 * <p>This manager coordinates leadership among multiple Cruise Control instances,
 * ensuring only one instance is active (leader) at any given time while others
 * remain in standby mode ready for failover.
 *
 * <p>Implementations may use different coordination services such as:
 * <ul>
 *   <li>ZooKeeper (via Curator framework)</li>
 *   <li>etcd (via lease mechanism)</li>
 *   <li>Kubernetes (via ConfigMaps/Leases)</li>
 * </ul>
 *
 * <p>Typical usage:
 * <pre>
 * LeaderElectionManager manager = new ZooKeeperLeaderElector(config);
 * manager.registerLeadershipListener(new LeadershipListener() {
 *     public void onLeadershipGained() {
 *         // Transition to active mode
 *     }
 *     public void onLeadershipLost() {
 *         // Transition to standby mode
 *     }
 * });
 * manager.start();
 * </pre>
 */
public interface LeaderElectionManager {
  /**
   * Start participating in leader election.
   * This method initiates the leader election process and begins competing
   * for leadership with other instances.
   *
   * @throws Exception if the leader election cannot be started
   */
  void start() throws Exception;

  /**
   * Register a callback for leadership change notifications.
   *
   * @param listener the listener to be notified of leadership changes
   */
  void registerLeadershipListener(LeadershipListener listener);

  /**
   * Check if this instance is currently the leader.
   *
   * @return true if this instance is the leader, false otherwise
   */
  boolean isLeader();

  /**
   * Get the identifier of the current leader instance.
   *
   * @return the instance ID of the current leader, or null if no leader exists
   */
  String getCurrentLeader();

  /**
   * Get this instance's identifier.
   *
   * @return this instance's ID
   */
  String getInstanceId();

  /**
   * Gracefully resign leadership.
   * This method allows the current leader to voluntarily give up leadership,
   * typically used during planned maintenance or rolling upgrades.
   *
   * <p>If this instance is not the leader, this method has no effect.
   */
  void resignLeadership();

  /**
   * Stop participating in leader election and release any resources.
   * After calling this method, the manager should not be reused.
   */
  void stop();

  /**
   * Check if the connection to the coordination service is healthy.
   *
   * @return true if connected to coordination service, false otherwise
   */
  boolean isCoordinationServiceHealthy();
}
