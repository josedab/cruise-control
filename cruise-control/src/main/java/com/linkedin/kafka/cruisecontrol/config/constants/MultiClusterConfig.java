/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Configuration constants for multi-cluster management.
 *
 * <p>These configurations control how Cruise Control manages multiple Kafka clusters
 * from a single instance.
 *
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class MultiClusterConfig {

  private MultiClusterConfig() {
  }

  /**
   * <code>multi.cluster.mode.enabled</code>
   */
  public static final String MULTI_CLUSTER_MODE_ENABLED_CONFIG = "multi.cluster.mode.enabled";
  public static final boolean DEFAULT_MULTI_CLUSTER_MODE_ENABLED = false;
  public static final String MULTI_CLUSTER_MODE_ENABLED_DOC = "Whether multi-cluster mode is enabled. "
      + "When false, Cruise Control operates in single-cluster mode (backward compatible).";

  /**
   * <code>clusters</code>
   */
  public static final String CLUSTERS_CONFIG = "clusters";
  public static final String DEFAULT_CLUSTERS = "";
  public static final String CLUSTERS_DOC = "Comma-separated list of cluster IDs to manage. "
      + "Example: us-east,us-west,eu-west";

  /**
   * <code>cluster.{id}.bootstrap.servers</code>
   */
  public static final String CLUSTER_BOOTSTRAP_SERVERS_TEMPLATE = "cluster.%s.bootstrap.servers";
  public static final String CLUSTER_BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for cluster {id}. "
      + "Example: cluster.us-east.bootstrap.servers=kafka1:9092,kafka2:9092";

  /**
   * <code>cluster.{id}.zookeeper.connect</code>
   */
  public static final String CLUSTER_ZOOKEEPER_CONNECT_TEMPLATE = "cluster.%s.zookeeper.connect";
  public static final String CLUSTER_ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string for cluster {id}. "
      + "Example: cluster.us-east.zookeeper.connect=zk1:2181,zk2:2181";

  /**
   * <code>cluster.{id}.display.name</code>
   */
  public static final String CLUSTER_DISPLAY_NAME_TEMPLATE = "cluster.%s.display.name";
  public static final String CLUSTER_DISPLAY_NAME_DOC = "Display name for cluster {id} (for UI). "
      + "Example: cluster.us-east.display.name=US East Production";

  /**
   * <code>cluster.{id}.region</code>
   */
  public static final String CLUSTER_REGION_TEMPLATE = "cluster.%s.region";
  public static final String CLUSTER_REGION_DOC = "Region/datacenter for cluster {id}. "
      + "Example: cluster.us-east.region=us-east-1";

  /**
   * <code>cluster.{id}.environment</code>
   */
  public static final String CLUSTER_ENVIRONMENT_TEMPLATE = "cluster.%s.environment";
  public static final String CLUSTER_ENVIRONMENT_DOC = "Environment for cluster {id} (production, staging, dev). "
      + "Example: cluster.us-east.environment=production";

  /**
   * <code>cluster.{id}.enabled</code>
   */
  public static final String CLUSTER_ENABLED_TEMPLATE = "cluster.%s.enabled";
  public static final String CLUSTER_ENABLED_DOC = "Whether cluster {id} is enabled for management. "
      + "Example: cluster.us-east.enabled=true";

  /**
   * <code>default.goals</code>
   */
  public static final String DEFAULT_GOALS_CONFIG = "default.goals";
  public static final String DEFAULT_GOALS_DOC = "Default goals to apply to all clusters unless overridden. "
      + "Can be overridden per cluster using cluster.{id}.goals";

  /**
   * <code>default.anomaly.detection.goals</code>
   */
  public static final String DEFAULT_ANOMALY_DETECTION_GOALS_CONFIG = "default.anomaly.detection.goals";
  public static final String DEFAULT_ANOMALY_DETECTION_GOALS_DOC = "Default anomaly detection goals for all clusters. "
      + "Can be overridden per cluster using cluster.{id}.anomaly.detection.goals";

  /**
   * <code>default.self.healing.enabled</code>
   */
  public static final String DEFAULT_SELF_HEALING_ENABLED_CONFIG = "default.self.healing.enabled";
  public static final String DEFAULT_SELF_HEALING_ENABLED_DOC = "Default self-healing setting for all clusters. "
      + "Can be overridden per cluster using cluster.{id}.self.healing.enabled";

  /**
   * Defines multi-cluster configuration.
   *
   * @param configDef Config definition.
   * @return the given ConfigDef after defining the configurations for multi-cluster.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef
        .define(MULTI_CLUSTER_MODE_ENABLED_CONFIG,
                BOOLEAN,
                DEFAULT_MULTI_CLUSTER_MODE_ENABLED,
                MEDIUM,
                MULTI_CLUSTER_MODE_ENABLED_DOC)
        .define(CLUSTERS_CONFIG,
                STRING,
                DEFAULT_CLUSTERS,
                MEDIUM,
                CLUSTERS_DOC);
  }
}
