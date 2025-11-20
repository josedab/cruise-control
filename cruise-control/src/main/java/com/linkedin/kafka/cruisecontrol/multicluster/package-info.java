/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

/**
 * Multi-cluster management support for Cruise Control.
 *
 * <p>This package provides the infrastructure to manage multiple Kafka clusters from a single
 * Cruise Control instance. It includes:
 *
 * <ul>
 *   <li>{@link com.linkedin.kafka.cruisecontrol.multicluster.ClusterConfig} - Configuration for individual clusters</li>
 *   <li>{@link com.linkedin.kafka.cruisecontrol.multicluster.ClusterRegistry} - Registry of all managed clusters</li>
 *   <li>{@link com.linkedin.kafka.cruisecontrol.multicluster.ClusterContext} - Encapsulates state for one cluster</li>
 *   <li>{@link com.linkedin.kafka.cruisecontrol.multicluster.MultiClusterCoordinator} - Orchestrates multi-cluster operations</li>
 *   <li>{@link com.linkedin.kafka.cruisecontrol.multicluster.MultiClusterConfigLoader} - Configuration loading utilities</li>
 * </ul>
 *
 * <h2>Key Concepts</h2>
 *
 * <h3>Cluster Isolation</h3>
 * Each cluster maintains complete isolation:
 * <ul>
 *   <li>Separate {@link com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor} for metric collection</li>
 *   <li>Independent {@link com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer} for proposal generation</li>
 *   <li>Isolated {@link com.linkedin.kafka.cruisecontrol.executor.Executor} for partition movements</li>
 *   <li>Separate {@link com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager} for self-healing</li>
 * </ul>
 *
 * <h3>Configuration Hierarchy</h3>
 * Configuration follows a three-level hierarchy:
 * <ol>
 *   <li><b>Global defaults</b> - Apply to all clusters unless overridden</li>
 *   <li><b>Cluster-specific</b> - Override defaults for specific clusters</li>
 *   <li><b>Runtime</b> - Can be modified without restart (future enhancement)</li>
 * </ol>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // Load configuration
 * Properties props = new Properties();
 * props.load(new FileInputStream("cruise-control.properties"));
 *
 * // Create coordinator
 * MetricRegistry metricRegistry = new MetricRegistry();
 * MultiClusterCoordinator coordinator =
 *     MultiClusterConfigLoader.loadCoordinator(props, metricRegistry);
 *
 * // Initialize all clusters
 * coordinator.initialize();
 *
 * // Get health across all clusters
 * Map<String, ClusterHealth> health = coordinator.getAllHealth();
 *
 * // Operate on specific cluster
 * ClusterContext usEast = coordinator.getContext("us-east");
 * OptimizerResult result = usEast.getOptimizationProposals(options);
 *
 * // Execute on all clusters
 * coordinator.executeOnAll(context -> {
 *     context.loadMonitor().bootstrap();
 *     return null;
 * });
 *
 * // Shutdown
 * coordinator.shutdown();
 * }</pre>
 *
 * <h2>Configuration Example</h2>
 *
 * <pre>
 * # Enable multi-cluster mode
 * multi.cluster.mode.enabled=true
 * clusters=us-east,us-west,eu-west
 *
 * # US-East cluster
 * cluster.us-east.bootstrap.servers=kafka1:9092,kafka2:9092
 * cluster.us-east.zookeeper.connect=zk1:2181,zk2:2181
 * cluster.us-east.display.name=US East Production
 * cluster.us-east.region=us-east-1
 * cluster.us-east.environment=production
 *
 * # Default configuration (inherited by all clusters)
 * default.goals=RackAwareGoal,ReplicaCapacityGoal,...
 * default.self.healing.enabled=true
 *
 * # Per-cluster overrides
 * cluster.eu-west.goals=RackAwareGoal,MinTopicLeadersPerBrokerGoal,...
 * </pre>
 *
 * <h2>Thread Safety</h2>
 *
 * All classes in this package are thread-safe and can be safely accessed from multiple threads.
 * The {@link com.linkedin.kafka.cruisecontrol.multicluster.MultiClusterCoordinator} uses parallel
 * streams for executing operations across clusters concurrently.
 *
 * <h2>Error Handling</h2>
 *
 * Operations on individual clusters are isolated. If one cluster fails, others continue operating
 * normally. The coordinator logs errors but does not propagate exceptions from individual cluster
 * operations to avoid cascading failures.
 *
 * @see com.linkedin.kafka.cruisecontrol.KafkaCruiseControl
 * @see com.linkedin.kafka.cruisecontrol.config.constants.MultiClusterConfig
 * @since 2.6.0
 */
package com.linkedin.kafka.cruisecontrol.multicluster;
