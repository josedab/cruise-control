/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.multicluster;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for a single Kafka cluster managed by multi-cluster Cruise Control.
 *
 * <p>This class encapsulates all cluster-specific configuration including:
 * <ul>
 *   <li>Cluster identification (ID, display name, region, environment)</li>
 *   <li>Connection details (bootstrap servers, zookeeper)</li>
 *   <li>Cluster-specific configuration overrides</li>
 * </ul>
 *
 * <p>Example configuration in properties file:
 * <pre>
 * cluster.us-east.bootstrap.servers=kafka1:9092,kafka2:9092
 * cluster.us-east.zookeeper.connect=zk1:2181,zk2:2181
 * cluster.us-east.display.name=US East Production
 * cluster.us-east.region=us-east-1
 * cluster.us-east.environment=production
 * cluster.us-east.enabled=true
 * </pre>
 */
public class ClusterConfig {
  private final String _clusterId;
  private final String _displayName;
  private final String _bootstrapServers;
  private final String _zookeeperConnect;
  private final String _region;
  private final String _environment;
  private final boolean _enabled;
  private final Map<String, Object> _configOverrides;
  private final KafkaCruiseControlConfig _cruiseControlConfig;

  private ClusterConfig(Builder builder) {
    this._clusterId = Objects.requireNonNull(builder._clusterId, "Cluster ID cannot be null");
    this._displayName = builder._displayName != null ? builder._displayName : builder._clusterId;
    this._bootstrapServers = Objects.requireNonNull(builder._bootstrapServers, "Bootstrap servers cannot be null");
    this._zookeeperConnect = Objects.requireNonNull(builder._zookeeperConnect, "Zookeeper connect cannot be null");
    this._region = builder._region;
    this._environment = builder._environment;
    this._enabled = builder._enabled;
    this._configOverrides = new HashMap<>(builder._configOverrides);
    this._cruiseControlConfig = builder._cruiseControlConfig;
  }

  /**
   * @return The unique identifier for this cluster.
   */
  public String clusterId() {
    return _clusterId;
  }

  /**
   * @return The display name for this cluster (for UI).
   */
  public String displayName() {
    return _displayName;
  }

  /**
   * @return The Kafka bootstrap servers for this cluster.
   */
  public String bootstrapServers() {
    return _bootstrapServers;
  }

  /**
   * @return The Zookeeper connect string for this cluster.
   */
  public String zookeeperConnect() {
    return _zookeeperConnect;
  }

  /**
   * @return The region/datacenter where this cluster is located (e.g., "us-east-1", "eu-west-1").
   */
  public String region() {
    return _region;
  }

  /**
   * @return The environment this cluster belongs to (e.g., "production", "staging", "dev").
   */
  public String environment() {
    return _environment;
  }

  /**
   * @return Whether this cluster is enabled for management.
   */
  public boolean enabled() {
    return _enabled;
  }

  /**
   * @return Cluster-specific configuration overrides.
   */
  public Map<String, Object> configOverrides() {
    return new HashMap<>(_configOverrides);
  }

  /**
   * Converts this cluster configuration to a full KafkaCruiseControlConfig by merging
   * with base configuration and applying cluster-specific overrides.
   *
   * @return KafkaCruiseControlConfig for this cluster
   */
  public KafkaCruiseControlConfig toKafkaConfig() {
    if (_cruiseControlConfig != null) {
      return _cruiseControlConfig;
    }

    // Create merged configuration with cluster-specific overrides
    Map<String, Object> mergedConfig = new HashMap<>(_configOverrides);
    mergedConfig.put("bootstrap.servers", _bootstrapServers);
    mergedConfig.put("zookeeper.connect", _zookeeperConnect);

    return new KafkaCruiseControlConfig(mergedConfig);
  }

  /**
   * Creates a builder for constructing ClusterConfig instances.
   *
   * @param clusterId The unique cluster identifier
   * @return A new builder instance
   */
  public static Builder builder(String clusterId) {
    return new Builder(clusterId);
  }

  /**
   * Builder for ClusterConfig.
   */
  public static class Builder {
    private final String _clusterId;
    private String _displayName;
    private String _bootstrapServers;
    private String _zookeeperConnect;
    private String _region;
    private String _environment;
    private boolean _enabled = true;
    private Map<String, Object> _configOverrides = new HashMap<>();
    private KafkaCruiseControlConfig _cruiseControlConfig;

    private Builder(String clusterId) {
      this._clusterId = clusterId;
    }

    public Builder displayName(String displayName) {
      this._displayName = displayName;
      return this;
    }

    public Builder bootstrapServers(String bootstrapServers) {
      this._bootstrapServers = bootstrapServers;
      return this;
    }

    public Builder zookeeperConnect(String zookeeperConnect) {
      this._zookeeperConnect = zookeeperConnect;
      return this;
    }

    public Builder region(String region) {
      this._region = region;
      return this;
    }

    public Builder environment(String environment) {
      this._environment = environment;
      return this;
    }

    public Builder enabled(boolean enabled) {
      this._enabled = enabled;
      return this;
    }

    public Builder configOverrides(Map<String, Object> configOverrides) {
      this._configOverrides = new HashMap<>(configOverrides);
      return this;
    }

    public Builder addConfigOverride(String key, Object value) {
      this._configOverrides.put(key, value);
      return this;
    }

    public Builder cruiseControlConfig(KafkaCruiseControlConfig config) {
      this._cruiseControlConfig = config;
      return this;
    }

    public ClusterConfig build() {
      return new ClusterConfig(this);
    }
  }

  @Override
  public String toString() {
    return "ClusterConfig{"
        + "clusterId='" + _clusterId + '\''
        + ", displayName='" + _displayName + '\''
        + ", bootstrapServers='" + _bootstrapServers + '\''
        + ", region='" + _region + '\''
        + ", environment='" + _environment + '\''
        + ", enabled=" + _enabled
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterConfig that = (ClusterConfig) o;
    return Objects.equals(_clusterId, that._clusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_clusterId);
  }
}
