/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link SmartProposalCache}.
 */
public class SmartProposalCacheTest {

  private SmartProposalCache _cache;

  @Before
  public void setUp() {
    _cache = new SmartProposalCache();
  }

  @Test
  public void testDetectChangeTypeWithNullCachedModel() {
    ClusterModel currentModel = createMockClusterModel(100, 1000, 50);

    ClusterChangeType changeType = _cache.detectChangeType(null, currentModel);

    Assert.assertEquals("Should return MAJOR_TOPOLOGY_CHANGE when cached model is null",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testDetectChangeTypeMetricsOnly() {
    // Same cluster generation, different load generation = metrics only change
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(100, 1005);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 100, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect METRICS_ONLY change when only load generation changed",
        ClusterChangeType.METRICS_ONLY, changeType);
  }

  @Test
  public void testDetectChangeTypeSingleBrokerAdded() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 11, 100, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect SINGLE_BROKER_ADDED when exactly one broker added",
        ClusterChangeType.SINGLE_BROKER_ADDED, changeType);
  }

  @Test
  public void testDetectChangeTypeSingleBrokerRemoved() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 11, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 100, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect SINGLE_BROKER_REMOVED when exactly one broker removed",
        ClusterChangeType.SINGLE_BROKER_REMOVED, changeType);
  }

  @Test
  public void testDetectChangeTypeMultipleBrokersChanged() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(102, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 12, 100, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect MAJOR_TOPOLOGY_CHANGE when multiple brokers added",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testDetectChangeTypeTopicAdded() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 103, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect TOPIC_ADDED when topics added within threshold",
        ClusterChangeType.TOPIC_ADDED, changeType);
  }

  @Test
  public void testDetectChangeTypeManyTopicsAdded() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 120, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect MAJOR_TOPOLOGY_CHANGE when many topics added",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testDetectChangeTypeTopicRemoved() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 50);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 95, 50);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect MAJOR_TOPOLOGY_CHANGE when topics removed",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testDetectChangeTypePartitionAdded() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 1000);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 100, 1005);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect PARTITION_ADDED when partitions added within threshold",
        ClusterChangeType.PARTITION_ADDED, changeType);
  }

  @Test
  public void testDetectChangeTypeManyPartitionsAdded() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 1000);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 100, 1050);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect MAJOR_TOPOLOGY_CHANGE when many partitions added",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testDetectChangeTypePartitionRemoved() {
    ModelGeneration cachedGen = new ModelGeneration(100, 1000);
    ModelGeneration currentGen = new ModelGeneration(101, 1000);

    ClusterModel cachedModel = createMockClusterModel(cachedGen, 10, 100, 1000);
    ClusterModel currentModel = createMockClusterModel(currentGen, 10, 100, 995);

    ClusterChangeType changeType = _cache.detectChangeType(cachedModel, currentModel);

    Assert.assertEquals("Should detect MAJOR_TOPOLOGY_CHANGE when partitions removed",
        ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, changeType);
  }

  @Test
  public void testCanReuseProposalsWithNullCache() {
    boolean canReuse = _cache.canReuseProposals(ClusterChangeType.METRICS_ONLY, null);

    Assert.assertFalse("Should not reuse proposals when cache is null", canReuse);
  }

  @Test
  public void testCanReuseProposalsWithEmptyCache() {
    Set<ExecutionProposal> emptySet = new HashSet<>();
    boolean canReuse = _cache.canReuseProposals(ClusterChangeType.METRICS_ONLY, emptySet);

    Assert.assertFalse("Should not reuse proposals when cache is empty", canReuse);
  }

  @Test
  public void testCanReuseProposalsForMetricsOnly() {
    Set<ExecutionProposal> proposals = createMockProposals(5);
    boolean canReuse = _cache.canReuseProposals(ClusterChangeType.METRICS_ONLY, proposals);

    Assert.assertTrue("Should reuse proposals for METRICS_ONLY changes", canReuse);
  }

  @Test
  public void testCanReuseProposalsForTopicAdded() {
    Set<ExecutionProposal> proposals = createMockProposals(5);
    boolean canReuse = _cache.canReuseProposals(ClusterChangeType.TOPIC_ADDED, proposals);

    Assert.assertTrue("Should reuse proposals for TOPIC_ADDED changes", canReuse);
  }

  @Test
  public void testCannotReuseProposalsForMajorChange() {
    Set<ExecutionProposal> proposals = createMockProposals(5);
    boolean canReuse = _cache.canReuseProposals(ClusterChangeType.MAJOR_TOPOLOGY_CHANGE, proposals);

    Assert.assertFalse("Should not reuse proposals for MAJOR_TOPOLOGY_CHANGE", canReuse);
  }

  @Test
  public void testUpdateProposalEstimates() {
    Set<ExecutionProposal> cachedProposals = createMockProposals(10);
    ClusterModel currentModel = createMockClusterModel(100, 1000, 50);

    Set<ExecutionProposal> updated = _cache.updateProposalEstimates(cachedProposals, currentModel);

    Assert.assertSame("Should return same proposals for metrics-only update",
        cachedProposals, updated);
    Assert.assertEquals("Proposal count should remain unchanged", 10, updated.size());
  }

  @Test
  public void testShouldRegenerateProposalsForMetricsOnly() {
    boolean shouldRegenerate = _cache.shouldRegenerateProposals(ClusterChangeType.METRICS_ONLY);

    Assert.assertFalse("Should not regenerate for METRICS_ONLY changes", shouldRegenerate);
  }

  @Test
  public void testShouldRegenerateProposalsForTopicAdded() {
    boolean shouldRegenerate = _cache.shouldRegenerateProposals(ClusterChangeType.TOPIC_ADDED);

    Assert.assertFalse("Should not regenerate for TOPIC_ADDED changes", shouldRegenerate);
  }

  @Test
  public void testShouldRegenerateProposalsForMajorChange() {
    boolean shouldRegenerate = _cache.shouldRegenerateProposals(ClusterChangeType.MAJOR_TOPOLOGY_CHANGE);

    Assert.assertTrue("Should regenerate for MAJOR_TOPOLOGY_CHANGE", shouldRegenerate);
  }

  /**
   * Creates a mock ClusterModel with specified generation and basic topology.
   */
  private ClusterModel createMockClusterModel(ModelGeneration generation,
                                               int numBrokers,
                                               int numTopics,
                                               int numReplicas) {
    ClusterModel model = EasyMock.mock(ClusterModel.class);

    EasyMock.expect(model.generation()).andReturn(generation).anyTimes();

    // Create broker set
    SortedSet<Broker> brokers = new TreeSet<>();
    for (int i = 0; i < numBrokers; i++) {
      Broker broker = EasyMock.mock(Broker.class);
      EasyMock.expect(broker.id()).andReturn(i).anyTimes();
      EasyMock.replay(broker);
      brokers.add(broker);
    }
    EasyMock.expect(model.brokers()).andReturn(brokers).anyTimes();

    // Create topic set
    Set<String> topics = new HashSet<>();
    for (int i = 0; i < numTopics; i++) {
      topics.add("topic-" + i);
    }
    EasyMock.expect(model.topics()).andReturn(topics).anyTimes();

    // Set replica count
    EasyMock.expect(model.numReplicas()).andReturn(numReplicas).anyTimes();

    EasyMock.replay(model);
    return model;
  }

  /**
   * Convenience method to create mock cluster model with simple generation.
   */
  private ClusterModel createMockClusterModel(long clusterGeneration,
                                               long loadGeneration,
                                               int numTopics) {
    ModelGeneration generation = new ModelGeneration(clusterGeneration, loadGeneration);
    return createMockClusterModel(generation, 10, numTopics, 1000);
  }

  /**
   * Creates a set of mock ExecutionProposal objects.
   */
  private Set<ExecutionProposal> createMockProposals(int count) {
    Set<ExecutionProposal> proposals = new HashSet<>();
    for (int i = 0; i < count; i++) {
      ExecutionProposal proposal = EasyMock.mock(ExecutionProposal.class);
      EasyMock.replay(proposal);
      proposals.add(proposal);
    }
    return proposals;
  }
}
