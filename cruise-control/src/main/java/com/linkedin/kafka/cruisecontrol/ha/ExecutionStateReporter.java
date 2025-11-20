/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reporter for publishing execution state checkpoints to Kafka.
 *
 * <p>This class is responsible for writing execution state to a dedicated Kafka topic,
 * allowing standby Cruise Control instances to stay synchronized with the leader's
 * execution state for fast failover.
 *
 * <p>Checkpoints are published asynchronously but with durability guarantees:
 * <ul>
 *   <li>acks=all - requires acknowledgment from all in-sync replicas</li>
 *   <li>Idempotent producer - prevents duplicate checkpoints</li>
 *   <li>Retries enabled - handles transient failures</li>
 * </ul>
 */
public class ExecutionStateReporter {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionStateReporter.class);
  private static final int CHECKPOINT_WRITE_TIMEOUT_MS = 5000;

  private final String _stateTopic;
  private final Producer<String, byte[]> _producer;
  private final boolean _enabled;

  /**
   * Create an execution state reporter.
   *
   * @param bootstrapServers Kafka bootstrap servers
   * @param stateTopic topic name for execution state
   * @param enabled whether checkpointing is enabled
   */
  public ExecutionStateReporter(String bootstrapServers, String stateTopic, boolean enabled) {
    this._stateTopic = stateTopic;
    this._enabled = enabled;

    if (_enabled) {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.RETRIES_CONFIG, 3);
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Ensure ordering
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "cruise-control-ha-state-reporter");

      this._producer = new KafkaProducer<>(props);
      LOG.info("ExecutionStateReporter initialized with topic: {}", _stateTopic);
    } else {
      this._producer = null;
      LOG.info("ExecutionStateReporter disabled");
    }
  }

  /**
   * Publish an execution checkpoint to the state topic.
   *
   * <p>This method blocks until the checkpoint is successfully written or times out.
   * The checkpoint key is the execution UUID to allow log compaction.
   *
   * @param checkpoint the execution checkpoint to publish
   * @return true if checkpoint was successfully published, false otherwise
   */
  public boolean checkpoint(ExecutionCheckpoint checkpoint) {
    if (!_enabled) {
      LOG.trace("Checkpointing disabled, skipping checkpoint for UUID: {}", checkpoint.uuid());
      return true;
    }

    try {
      byte[] value = checkpoint.toJson();
      ProducerRecord<String, byte[]> record = new ProducerRecord<>(
          _stateTopic,
          checkpoint.uuid(), // Use UUID as key for log compaction
          value
      );

      LOG.debug("Publishing checkpoint for execution {}: state={}, hasOngoing={}",
               checkpoint.uuid(), checkpoint.executorState(), checkpoint.hasOngoingExecution());

      Future<RecordMetadata> future = _producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          LOG.error("Failed to publish checkpoint for execution {}", checkpoint.uuid(), exception);
        } else {
          LOG.trace("Checkpoint published successfully: topic={}, partition={}, offset={}",
                   metadata.topic(), metadata.partition(), metadata.offset());
        }
      });

      // Wait for acknowledgment with timeout
      future.get(CHECKPOINT_WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;

    } catch (InterruptedException e) {
      LOG.error("Interrupted while publishing checkpoint for execution {}", checkpoint.uuid(), e);
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      LOG.error("Failed to publish checkpoint for execution {}", checkpoint.uuid(), e.getCause());
      return false;
    } catch (TimeoutException e) {
      LOG.error("Timeout publishing checkpoint for execution {} after {}ms",
               checkpoint.uuid(), CHECKPOINT_WRITE_TIMEOUT_MS);
      return false;
    } catch (Exception e) {
      LOG.error("Unexpected error publishing checkpoint for execution {}", checkpoint.uuid(), e);
      return false;
    }
  }

  /**
   * Close the reporter and release resources.
   */
  public void close() {
    if (_producer != null) {
      try {
        _producer.close(10, TimeUnit.SECONDS);
        LOG.info("ExecutionStateReporter closed");
      } catch (Exception e) {
        LOG.error("Error closing ExecutionStateReporter", e);
      }
    }
  }
}
