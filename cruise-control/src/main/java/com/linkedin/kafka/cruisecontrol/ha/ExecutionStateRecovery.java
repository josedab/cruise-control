/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.ha;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovery mechanism for restoring execution state from Kafka topics.
 *
 * <p>This class consumes execution state checkpoints from the HA state topic
 * and reconstructs the latest state for each execution UUID. This allows a
 * standby instance to resume operations when it becomes the leader.
 *
 * <p>The recovery process:
 * <ol>
 *   <li>Consume all records from the state topic (from beginning)</li>
 *   <li>Build a map of UUID -> latest checkpoint</li>
 *   <li>Return the most recent checkpoint(s) for active executions</li>
 * </ol>
 */
public class ExecutionStateRecovery {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionStateRecovery.class);
  private static final int RECOVERY_POLL_TIMEOUT_MS = 1000;

  private final String _stateTopic;
  private final String _bootstrapServers;
  private final long _recoveryTimeoutMs;

  /**
   * Create an execution state recovery manager.
   *
   * @param bootstrapServers Kafka bootstrap servers
   * @param stateTopic topic name for execution state
   * @param recoveryTimeoutMs maximum time to wait for recovery
   */
  public ExecutionStateRecovery(String bootstrapServers, String stateTopic, long recoveryTimeoutMs) {
    this._bootstrapServers = bootstrapServers;
    this._stateTopic = stateTopic;
    this._recoveryTimeoutMs = recoveryTimeoutMs;
  }

  /**
   * Recover execution state by consuming all checkpoints from the state topic.
   *
   * <p>This method reads from the beginning of the topic to build a complete
   * picture of execution state. It returns a map of execution UUID to the
   * most recent checkpoint for that execution.
   *
   * @return map of execution UUID to latest checkpoint
   * @throws Exception if recovery fails or times out
   */
  public Map<String, ExecutionCheckpoint> recoverState() throws Exception {
    LOG.info("Starting execution state recovery from topic: {}", _stateTopic);
    long startTime = System.currentTimeMillis();

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cruise-control-ha-recovery-" + System.currentTimeMillis());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from beginning
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "cruise-control-ha-state-recovery");

    Map<String, ExecutionCheckpoint> latestCheckpoints = new HashMap<>();

    try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
      // Subscribe to the state topic
      consumer.subscribe(Collections.singletonList(_stateTopic));

      // Wait for assignment
      while (consumer.assignment().isEmpty()
             && System.currentTimeMillis() - startTime < _recoveryTimeoutMs) {
        consumer.poll(Duration.ofMillis(100));
      }

      if (consumer.assignment().isEmpty()) {
        LOG.warn("No partitions assigned for state topic {} within timeout", _stateTopic);
        return latestCheckpoints;
      }

      // Seek to beginning
      consumer.seekToBeginning(consumer.assignment());

      LOG.info("Reading execution state from {} partitions", consumer.assignment().size());

      // Track end offsets to know when we've consumed everything
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
      Map<TopicPartition, Long> currentOffsets = new HashMap<>();
      for (TopicPartition tp : consumer.assignment()) {
        currentOffsets.put(tp, consumer.position(tp));
      }

      int recordCount = 0;
      boolean reachedEnd = false;

      // Consume until we reach the end or timeout
      while (!reachedEnd && System.currentTimeMillis() - startTime < _recoveryTimeoutMs) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(RECOVERY_POLL_TIMEOUT_MS));

        for (ConsumerRecord<String, byte[]> record : records) {
          try {
            if (record.value() != null) {
              ExecutionCheckpoint checkpoint = ExecutionCheckpoint.fromJson(record.value());
              latestCheckpoints.put(checkpoint.uuid(), checkpoint);
              recordCount++;
              LOG.trace("Recovered checkpoint: {}", checkpoint);
            }
          } catch (Exception e) {
            LOG.error("Failed to deserialize checkpoint at offset {} in partition {}",
                     record.offset(), record.partition(), e);
          }
        }

        // Update current positions
        for (TopicPartition tp : consumer.assignment()) {
          currentOffsets.put(tp, consumer.position(tp));
        }

        // Check if we've reached the end of all partitions
        reachedEnd = true;
        for (TopicPartition tp : consumer.assignment()) {
          if (currentOffsets.get(tp) < endOffsets.get(tp)) {
            reachedEnd = false;
            break;
          }
        }
      }

      long elapsedMs = System.currentTimeMillis() - startTime;
      LOG.info("Execution state recovery completed: {} checkpoints recovered from {} records in {}ms",
               latestCheckpoints.size(), recordCount, elapsedMs);

      // Log summary of recovered state
      for (ExecutionCheckpoint checkpoint : latestCheckpoints.values()) {
        LOG.info("Recovered execution: uuid={}, state={}, hasOngoing={}, time={}",
                checkpoint.uuid(), checkpoint.executorState(),
                checkpoint.hasOngoingExecution(), checkpoint.checkpointTimeMs());
      }

      return latestCheckpoints;

    } catch (Exception e) {
      LOG.error("Failed to recover execution state", e);
      throw e;
    }
  }

  /**
   * Get the most recent active execution checkpoint.
   *
   * <p>This is a convenience method that returns the single most recent
   * checkpoint with ongoing execution, or null if none exists.
   *
   * @return the most recent active checkpoint, or null
   * @throws Exception if recovery fails
   */
  public ExecutionCheckpoint recoverActiveExecution() throws Exception {
    Map<String, ExecutionCheckpoint> checkpoints = recoverState();

    ExecutionCheckpoint mostRecent = null;
    for (ExecutionCheckpoint checkpoint : checkpoints.values()) {
      if (checkpoint.hasOngoingExecution()) {
        if (mostRecent == null || checkpoint.checkpointTimeMs() > mostRecent.checkpointTimeMs()) {
          mostRecent = checkpoint;
        }
      }
    }

    if (mostRecent != null) {
      LOG.info("Found active execution to recover: {}", mostRecent);
    } else {
      LOG.info("No active execution found to recover");
    }

    return mostRecent;
  }
}
