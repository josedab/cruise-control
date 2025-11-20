/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.EXECUTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;

/**
 * Tracks metrics for execution operations.
 *
 * <p>This class centralizes all execution-related metrics including:
 * <ul>
 *   <li>Task lifecycle metrics (submitted, completed, failed)</li>
 *   <li>Data movement metrics (bytes transferred, throughput)</li>
 *   <li>Performance metrics (task duration, movement rates)</li>
 * </ul>
 */
public class ExecutionMetrics {
  // Metric name constants from ExecutionTaskTracker
  public static final String INTER_BROKER_REPLICA_ACTION = "replica-action";
  public static final String INTRA_BROKER_REPLICA_ACTION = "intra-broker-replica-action";
  public static final String LEADERSHIP_ACTION = "leadership-action";
  public static final String IN_PROGRESS = "in-progress";
  public static final String PENDING = "pending";
  public static final String ABORTING = "aborting";
  public static final String ABORTED = "aborted";
  public static final String DEAD = "dead";
  public static final String COMPLETED = "completed";
  public static final String GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE = "ongoing-execution-kafka_assigner";
  public static final String GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE = "ongoing-execution-non_kafka_assigner";
  public static final String METER_INTER_BROKER_PARTITION_MOVEMENT_RATE = "inter-broker-partition-movement-rate";
  public static final String METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE = "intra-broker-partition-movement-rate";
  public static final String METER_LEADERSHIP_MOVEMENT_RATE = "leadership-movement-rate";
  public static final String METER_PARTITION_DATA_MOVEMENT_RATE = "partition-data-movement-rate-MB";

  private final MetricRegistry _registry;

  // Gauges for task counts by type and state
  private final AtomicInteger _tasksInProgress = new AtomicInteger(0);
  private final AtomicInteger _tasksPending = new AtomicInteger(0);
  private final AtomicInteger _tasksCompleted = new AtomicInteger(0);
  private final AtomicInteger _tasksFailed = new AtomicInteger(0);
  private final AtomicInteger _tasksAborting = new AtomicInteger(0);
  private final AtomicInteger _tasksAborted = new AtomicInteger(0);
  private final AtomicInteger _tasksDead = new AtomicInteger(0);

  // Data movement metrics
  private final AtomicLong _remainingInterBrokerDataToMoveInMB = new AtomicLong(0);
  private final AtomicLong _remainingIntraBrokerDataToMoveInMB = new AtomicLong(0);
  private final AtomicLong _inExecutionInterBrokerDataMovementInMB = new AtomicLong(0);
  private final AtomicLong _inExecutionIntraBrokerDataMovementInMB = new AtomicLong(0);
  private final AtomicLong _finishedInterBrokerDataMovementInMB = new AtomicLong(0);
  private final AtomicLong _finishedIntraBrokerDataMovementInMB = new AtomicLong(0);

  // Meters for throughput
  private final Meter _interBrokerPartitionMovementRateMeter;
  private final Meter _intraBrokerPartitionMovementRateMeter;
  private final Meter _leadershipMovementRateMeter;
  private final Meter _partitionDataMovementRateMeter;

  // Counters
  private final Counter _tasksSubmitted;
  private final Counter _kafkaApiCalls;
  private final Counter _kafkaApiSuccesses;
  private final Counter _kafkaApiFailures;

  // Histograms for performance tracking
  private final Histogram _taskDuration;
  private final Histogram _throughput;

  /**
   * Creates a new ExecutionMetrics instance.
   *
   * @param registry the metric registry to register metrics with
   */
  public ExecutionMetrics(MetricRegistry registry) {
    _registry = registry;

    // Initialize meters
    _interBrokerPartitionMovementRateMeter = new Meter();
    _intraBrokerPartitionMovementRateMeter = new Meter();
    _leadershipMovementRateMeter = new Meter();
    _partitionDataMovementRateMeter = new Meter();

    // Initialize counters
    _tasksSubmitted = new Counter();
    _kafkaApiCalls = new Counter();
    _kafkaApiSuccesses = new Counter();
    _kafkaApiFailures = new Counter();

    // Initialize histograms
    _taskDuration = new Histogram(new com.codahale.metrics.SlidingWindowReservoir(1000));
    _throughput = new Histogram(new com.codahale.metrics.SlidingWindowReservoir(1000));

    // Register all metrics
    registerMetrics();
  }

  /**
   * Registers all metrics with the registry.
   */
  private void registerMetrics() {
    // Register task count gauges
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-in-progress"),
        (Gauge<Integer>) _tasksInProgress::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-pending"),
        (Gauge<Integer>) _tasksPending::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-completed"),
        (Gauge<Integer>) _tasksCompleted::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-failed"),
        (Gauge<Integer>) _tasksFailed::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-aborting"),
        (Gauge<Integer>) _tasksAborting::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-aborted"),
        (Gauge<Integer>) _tasksAborted::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-dead"),
        (Gauge<Integer>) _tasksDead::get);

    // Register data movement gauges
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "remaining-inter-broker-data-to-move-mb"),
        (Gauge<Long>) _remainingInterBrokerDataToMoveInMB::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "remaining-intra-broker-data-to-move-mb"),
        (Gauge<Long>) _remainingIntraBrokerDataToMoveInMB::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "in-execution-inter-broker-data-movement-mb"),
        (Gauge<Long>) _inExecutionInterBrokerDataMovementInMB::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "in-execution-intra-broker-data-movement-mb"),
        (Gauge<Long>) _inExecutionIntraBrokerDataMovementInMB::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "finished-inter-broker-data-movement-mb"),
        (Gauge<Long>) _finishedInterBrokerDataMovementInMB::get);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "finished-intra-broker-data-movement-mb"),
        (Gauge<Long>) _finishedIntraBrokerDataMovementInMB::get);

    // Register meters
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_INTER_BROKER_PARTITION_MOVEMENT_RATE),
        _interBrokerPartitionMovementRateMeter);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE),
        _intraBrokerPartitionMovementRateMeter);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_LEADERSHIP_MOVEMENT_RATE),
        _leadershipMovementRateMeter);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_PARTITION_DATA_MOVEMENT_RATE),
        _partitionDataMovementRateMeter);

    // Register counters
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "tasks-submitted"),
        _tasksSubmitted);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "kafka-api-calls"),
        _kafkaApiCalls);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "kafka-api-successes"),
        _kafkaApiSuccesses);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "kafka-api-failures"),
        _kafkaApiFailures);

    // Register histograms
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "task-duration-ms"),
        _taskDuration);
    _registry.register(MetricRegistry.name(EXECUTOR_SENSOR, "throughput-bytes"),
        _throughput);
  }

  /**
   * Records that a task has been submitted.
   */
  public void recordTaskSubmitted() {
    _tasksSubmitted.inc();
    _tasksPending.incrementAndGet();
  }

  /**
   * Records that a task has completed.
   *
   * @param durationMs the task duration in milliseconds
   */
  public void recordTaskCompleted(long durationMs) {
    _taskDuration.update(durationMs);
    _tasksCompleted.incrementAndGet();
  }

  /**
   * Records that a task has failed.
   */
  public void recordTaskFailed() {
    _tasksFailed.incrementAndGet();
  }

  /**
   * Records bytes transferred during execution.
   *
   * @param bytes the number of bytes transferred
   */
  public void recordBytesTransferred(long bytes) {
    _throughput.update(bytes);
    _partitionDataMovementRateMeter.mark(bytes / (1024 * 1024)); // Convert to MB
  }

  /**
   * Records a Kafka API call.
   *
   * @param apiName the name of the API called
   */
  public void recordApiCall(String apiName) {
    _kafkaApiCalls.inc();
  }

  /**
   * Records a successful Kafka API call.
   *
   * @param apiName the name of the API that succeeded
   */
  public void recordApiSuccess(String apiName) {
    _kafkaApiSuccesses.inc();
  }

  /**
   * Records a failed Kafka API call.
   *
   * @param apiName the name of the API that failed
   */
  public void recordApiFailure(String apiName) {
    _kafkaApiFailures.inc();
  }

  /**
   * Records an inter-broker partition movement.
   */
  public void recordInterBrokerPartitionMovement() {
    _interBrokerPartitionMovementRateMeter.mark();
  }

  /**
   * Records an intra-broker partition movement.
   */
  public void recordIntraBrokerPartitionMovement() {
    _intraBrokerPartitionMovementRateMeter.mark();
  }

  /**
   * Records a leadership movement.
   */
  public void recordLeadershipMovement() {
    _leadershipMovementRateMeter.mark();
  }

  /**
   * Updates task count for a state transition.
   *
   * @param fromState the state transitioning from
   * @param toState the state transitioning to
   */
  public void recordTaskStateTransition(ExecutionTaskState fromState, ExecutionTaskState toState) {
    // Decrement old state count
    if (fromState != null) {
      decrementStateCount(fromState);
    }

    // Increment new state count
    incrementStateCount(toState);
  }

  private void incrementStateCount(ExecutionTaskState state) {
    switch (state) {
      case PENDING:
        _tasksPending.incrementAndGet();
        break;
      case IN_PROGRESS:
        _tasksInProgress.incrementAndGet();
        break;
      case ABORTING:
        _tasksAborting.incrementAndGet();
        break;
      case ABORTED:
        _tasksAborted.incrementAndGet();
        break;
      case DEAD:
        _tasksDead.incrementAndGet();
        break;
      case COMPLETED:
        _tasksCompleted.incrementAndGet();
        break;
    }
  }

  private void decrementStateCount(ExecutionTaskState state) {
    switch (state) {
      case PENDING:
        _tasksPending.decrementAndGet();
        break;
      case IN_PROGRESS:
        _tasksInProgress.decrementAndGet();
        break;
      case ABORTING:
        _tasksAborting.decrementAndGet();
        break;
      case ABORTED:
        _tasksAborted.decrementAndGet();
        break;
      case DEAD:
        _tasksDead.decrementAndGet();
        break;
      case COMPLETED:
        _tasksCompleted.decrementAndGet();
        break;
    }
  }

  /**
   * Gets current execution progress as percentage.
   *
   * @return progress from 0.0 to 1.0
   */
  public double getProgress() {
    int total = _tasksCompleted.get() +
               _tasksInProgress.get() +
               _tasksPending.get();
    if (total == 0) {
      return 0.0;
    }
    return (double) _tasksCompleted.get() / total;
  }

  /**
   * Updates remaining data to move.
   *
   * @param interBrokerDataMB remaining inter-broker data in MB
   * @param intraBrokerDataMB remaining intra-broker data in MB
   */
  public void updateRemainingDataToMove(long interBrokerDataMB, long intraBrokerDataMB) {
    _remainingInterBrokerDataToMoveInMB.set(interBrokerDataMB);
    _remainingIntraBrokerDataToMoveInMB.set(intraBrokerDataMB);
  }

  /**
   * Updates in-execution data movement.
   *
   * @param interBrokerDataMB in-execution inter-broker data in MB
   * @param intraBrokerDataMB in-execution intra-broker data in MB
   */
  public void updateInExecutionDataMovement(long interBrokerDataMB, long intraBrokerDataMB) {
    _inExecutionInterBrokerDataMovementInMB.set(interBrokerDataMB);
    _inExecutionIntraBrokerDataMovementInMB.set(intraBrokerDataMB);
  }

  /**
   * Updates finished data movement.
   *
   * @param interBrokerDataMB finished inter-broker data in MB
   * @param intraBrokerDataMB finished intra-broker data in MB
   */
  public void updateFinishedDataMovement(long interBrokerDataMB, long intraBrokerDataMB) {
    _finishedInterBrokerDataMovementInMB.set(interBrokerDataMB);
    _finishedIntraBrokerDataMovementInMB.set(intraBrokerDataMB);
  }

  /**
   * Gets the number of tasks in progress.
   *
   * @return task count
   */
  public int getTasksInProgress() {
    return _tasksInProgress.get();
  }

  /**
   * Gets the number of pending tasks.
   *
   * @return task count
   */
  public int getTasksPending() {
    return _tasksPending.get();
  }

  /**
   * Gets the number of completed tasks.
   *
   * @return task count
   */
  public int getTasksCompleted() {
    return _tasksCompleted.get();
  }

  /**
   * Gets the inter-broker partition movement rate meter.
   *
   * @return the meter
   */
  public Meter getInterBrokerPartitionMovementRateMeter() {
    return _interBrokerPartitionMovementRateMeter;
  }

  /**
   * Gets the intra-broker partition movement rate meter.
   *
   * @return the meter
   */
  public Meter getIntraBrokerPartitionMovementRateMeter() {
    return _intraBrokerPartitionMovementRateMeter;
  }

  /**
   * Gets the leadership movement rate meter.
   *
   * @return the meter
   */
  public Meter getLeadershipMovementRateMeter() {
    return _leadershipMovementRateMeter;
  }
}
