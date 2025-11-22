/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.simulator;

import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating deep copies of ClusterModel.
 * <p>
 * This class uses Java serialization to create complete deep copies of
 * ClusterModel instances. This is necessary for simulation to avoid modifying
 * the actual cluster state during what-if analysis.
 * </p>
 */
public final class ClusterModelCopier {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterModelCopier.class);

  private ClusterModelCopier() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates a deep copy of the given ClusterModel.
   * <p>
   * Uses Java serialization for deep copying. Since ClusterModel implements
   * Serializable, this approach ensures all nested objects are properly cloned.
   * </p>
   *
   * @param original the cluster model to copy
   * @return a deep copy of the cluster model
   * @throws SimulationException if the copy operation fails
   */
  public static ClusterModel copy(ClusterModel original) throws SimulationException {
    if (original == null) {
      throw new SimulationException("Cannot copy null ClusterModel");
    }

    LOG.debug("Creating deep copy of ClusterModel with {} brokers", original.brokers().size());
    long startTime = System.currentTimeMillis();

    try {
      // Serialize the object to a byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(original);
      oos.close();

      // Deserialize the byte array to a new object
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      ClusterModel copy = (ClusterModel) ois.readObject();
      ois.close();

      long duration = System.currentTimeMillis() - startTime;
      LOG.debug("ClusterModel copy completed in {} ms", duration);

      return copy;
    } catch (IOException | ClassNotFoundException e) {
      LOG.error("Failed to create deep copy of ClusterModel", e);
      throw new SimulationException("Failed to copy ClusterModel: " + e.getMessage(), e);
    }
  }

  /**
   * Validates that a cluster model copy is independent from the original.
   * <p>
   * This method performs basic sanity checks to verify that modifying the copy
   * does not affect the original. Used primarily for testing.
   * </p>
   *
   * @param original the original cluster model
   * @param copy the copied cluster model
   * @return true if the copy appears to be independent
   */
  public static boolean validateCopyIndependence(ClusterModel original, ClusterModel copy) {
    if (original == copy) {
      LOG.warn("Copy is the same object as original");
      return false;
    }

    // Check that basic properties match
    if (original.brokers().size() != copy.brokers().size()) {
      LOG.warn("Broker count mismatch: original={}, copy={}",
          original.brokers().size(), copy.brokers().size());
      return false;
    }

    if (original.partitions().size() != copy.partitions().size()) {
      LOG.warn("Partition count mismatch: original={}, copy={}",
          original.partitions().size(), copy.partitions().size());
      return false;
    }

    // Check that broker sets are independent (not the same object)
    if (original.brokers() == copy.brokers()) {
      LOG.warn("Broker sets are the same object");
      return false;
    }

    LOG.debug("Copy independence validation passed");
    return true;
  }
}
