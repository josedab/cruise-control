/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * A class to keep Cruise Control High Availability (HA) Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class HAConfig {

  /**
   * <code>cruise.control.ha.enabled</code>
   */
  public static final String HA_ENABLED_CONFIG = "cruise.control.ha.enabled";
  public static final boolean DEFAULT_HA_ENABLED = false;
  public static final String HA_ENABLED_DOC = "Enable high availability mode with leader election. "
      + "When enabled, multiple Cruise Control instances can run in active-passive mode "
      + "with automatic failover.";

  /**
   * <code>cruise.control.ha.coordination.service</code>
   */
  public static final String HA_COORDINATION_SERVICE_CONFIG = "cruise.control.ha.coordination.service";
  public static final String DEFAULT_HA_COORDINATION_SERVICE = "zookeeper";
  public static final String HA_COORDINATION_SERVICE_DOC = "Coordination service to use for leader election. "
      + "Supported values: 'zookeeper', 'etcd', 'kubernetes'. Default is 'zookeeper'.";

  /**
   * <code>cruise.control.ha.instance.id</code>
   */
  public static final String HA_INSTANCE_ID_CONFIG = "cruise.control.ha.instance.id";
  public static final String HA_INSTANCE_ID_DOC = "Unique identifier for this Cruise Control instance. "
      + "If not set, defaults to the hostname. Each instance in an HA cluster must have a unique ID.";

  /**
   * <code>cruise.control.ha.zookeeper.connect</code>
   */
  public static final String HA_ZOOKEEPER_CONNECT_CONFIG = "cruise.control.ha.zookeeper.connect";
  public static final String HA_ZOOKEEPER_CONNECT_DOC = "ZooKeeper connection string for leader election "
      + "(e.g., 'zk1:2181,zk2:2181,zk3:2181'). Required when coordination.service is 'zookeeper'.";

  /**
   * <code>cruise.control.ha.zookeeper.election.path</code>
   */
  public static final String HA_ZOOKEEPER_ELECTION_PATH_CONFIG = "cruise.control.ha.zookeeper.election.path";
  public static final String DEFAULT_HA_ZOOKEEPER_ELECTION_PATH = "/cruise-control/leader";
  public static final String HA_ZOOKEEPER_ELECTION_PATH_DOC = "ZooKeeper path for leader election. "
      + "All Cruise Control instances must use the same election path.";

  /**
   * <code>cruise.control.ha.zookeeper.session.timeout.ms</code>
   */
  public static final String HA_ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG = "cruise.control.ha.zookeeper.session.timeout.ms";
  public static final int DEFAULT_HA_ZOOKEEPER_SESSION_TIMEOUT_MS = 30000;
  public static final String HA_ZOOKEEPER_SESSION_TIMEOUT_MS_DOC = "ZooKeeper session timeout in milliseconds. "
      + "If the leader doesn't heartbeat within this time, it loses leadership and a new leader is elected.";

  /**
   * <code>cruise.control.ha.etcd.endpoints</code>
   */
  public static final String HA_ETCD_ENDPOINTS_CONFIG = "cruise.control.ha.etcd.endpoints";
  public static final String HA_ETCD_ENDPOINTS_DOC = "Comma-separated list of etcd endpoints "
      + "(e.g., 'http://etcd1:2379,http://etcd2:2379'). Required when coordination.service is 'etcd'.";

  /**
   * <code>cruise.control.ha.etcd.lease.ttl.seconds</code>
   */
  public static final String HA_ETCD_LEASE_TTL_SECONDS_CONFIG = "cruise.control.ha.etcd.lease.ttl.seconds";
  public static final int DEFAULT_HA_ETCD_LEASE_TTL_SECONDS = 15;
  public static final String HA_ETCD_LEASE_TTL_SECONDS_DOC = "etcd lease TTL in seconds for leader election. "
      + "The leader must refresh its lease within this time to maintain leadership.";

  /**
   * <code>cruise.control.ha.kubernetes.namespace</code>
   */
  public static final String HA_KUBERNETES_NAMESPACE_CONFIG = "cruise.control.ha.kubernetes.namespace";
  public static final String DEFAULT_HA_KUBERNETES_NAMESPACE = "default";
  public static final String HA_KUBERNETES_NAMESPACE_DOC = "Kubernetes namespace for leader election resources. "
      + "Required when coordination.service is 'kubernetes'.";

  /**
   * <code>cruise.control.ha.kubernetes.lease.name</code>
   */
  public static final String HA_KUBERNETES_LEASE_NAME_CONFIG = "cruise.control.ha.kubernetes.lease.name";
  public static final String DEFAULT_HA_KUBERNETES_LEASE_NAME = "cruise-control-leader";
  public static final String HA_KUBERNETES_LEASE_NAME_DOC = "Name of the Kubernetes Lease resource used for leader election.";

  /**
   * <code>cruise.control.ha.failover.timeout.ms</code>
   */
  public static final String HA_FAILOVER_TIMEOUT_MS_CONFIG = "cruise.control.ha.failover.timeout.ms";
  public static final int DEFAULT_HA_FAILOVER_TIMEOUT_MS = 10000;
  public static final String HA_FAILOVER_TIMEOUT_MS_DOC = "Maximum time in milliseconds for failover transition "
      + "from standby to active. If the transition takes longer, an error is logged.";

  /**
   * <code>cruise.control.ha.state.recovery.timeout.ms</code>
   */
  public static final String HA_STATE_RECOVERY_TIMEOUT_MS_CONFIG = "cruise.control.ha.state.recovery.timeout.ms";
  public static final int DEFAULT_HA_STATE_RECOVERY_TIMEOUT_MS = 60000;
  public static final String HA_STATE_RECOVERY_TIMEOUT_MS_DOC = "Maximum time in milliseconds to wait for "
      + "execution state recovery from Kafka topics during failover.";

  /**
   * <code>cruise.control.ha.execution.state.topic</code>
   */
  public static final String HA_EXECUTION_STATE_TOPIC_CONFIG = "cruise.control.ha.execution.state.topic";
  public static final String DEFAULT_HA_EXECUTION_STATE_TOPIC = "__CruiseControlExecutionState";
  public static final String HA_EXECUTION_STATE_TOPIC_DOC = "Kafka topic for storing execution state checkpoints. "
      + "Used to synchronize state between active and standby instances.";

  /**
   * <code>cruise.control.ha.execution.state.checkpoint.interval.ms</code>
   */
  public static final String HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS_CONFIG =
      "cruise.control.ha.execution.state.checkpoint.interval.ms";
  public static final int DEFAULT_HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS = 10000;
  public static final String HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS_DOC = "Interval in milliseconds "
      + "for checkpointing execution state to the state topic. Lower values reduce potential data loss "
      + "during failover but increase write overhead.";

  private HAConfig() {
  }

  /**
   * Define configs for high availability.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for high availability.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef
        .define(HA_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_HA_ENABLED,
                ConfigDef.Importance.HIGH,
                HA_ENABLED_DOC)
        .define(HA_COORDINATION_SERVICE_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_HA_COORDINATION_SERVICE,
                ConfigDef.Importance.HIGH,
                HA_COORDINATION_SERVICE_DOC)
        .define(HA_INSTANCE_ID_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                HA_INSTANCE_ID_DOC)
        .define(HA_ZOOKEEPER_CONNECT_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                HA_ZOOKEEPER_CONNECT_DOC)
        .define(HA_ZOOKEEPER_ELECTION_PATH_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_HA_ZOOKEEPER_ELECTION_PATH,
                ConfigDef.Importance.MEDIUM,
                HA_ZOOKEEPER_ELECTION_PATH_DOC)
        .define(HA_ZOOKEEPER_SESSION_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_HA_ZOOKEEPER_SESSION_TIMEOUT_MS,
                atLeast(1000),
                ConfigDef.Importance.MEDIUM,
                HA_ZOOKEEPER_SESSION_TIMEOUT_MS_DOC)
        .define(HA_ETCD_ENDPOINTS_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                HA_ETCD_ENDPOINTS_DOC)
        .define(HA_ETCD_LEASE_TTL_SECONDS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_HA_ETCD_LEASE_TTL_SECONDS,
                atLeast(5),
                ConfigDef.Importance.MEDIUM,
                HA_ETCD_LEASE_TTL_SECONDS_DOC)
        .define(HA_KUBERNETES_NAMESPACE_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_HA_KUBERNETES_NAMESPACE,
                ConfigDef.Importance.MEDIUM,
                HA_KUBERNETES_NAMESPACE_DOC)
        .define(HA_KUBERNETES_LEASE_NAME_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_HA_KUBERNETES_LEASE_NAME,
                ConfigDef.Importance.MEDIUM,
                HA_KUBERNETES_LEASE_NAME_DOC)
        .define(HA_FAILOVER_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_HA_FAILOVER_TIMEOUT_MS,
                atLeast(1000),
                ConfigDef.Importance.MEDIUM,
                HA_FAILOVER_TIMEOUT_MS_DOC)
        .define(HA_STATE_RECOVERY_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_HA_STATE_RECOVERY_TIMEOUT_MS,
                atLeast(1000),
                ConfigDef.Importance.MEDIUM,
                HA_STATE_RECOVERY_TIMEOUT_MS_DOC)
        .define(HA_EXECUTION_STATE_TOPIC_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_HA_EXECUTION_STATE_TOPIC,
                ConfigDef.Importance.MEDIUM,
                HA_EXECUTION_STATE_TOPIC_DOC)
        .define(HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS,
                atLeast(1000),
                ConfigDef.Importance.LOW,
                HA_EXECUTION_STATE_CHECKPOINT_INTERVAL_MS_DOC);
  }
}
