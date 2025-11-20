/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for ClusterModel operations.
 */
public class ClusterModelUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterModelUtils.class);

  private ClusterModelUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Creates a deep copy of a ClusterModel using Java serialization.
   * <p>
   *   This is not the most efficient approach, but it works since ClusterModel implements Serializable.
   *   Future enhancement: implement a native deep copy method in ClusterModel for better performance.
   * </p>
   *
   * @param original The original ClusterModel to copy
   * @return A deep copy of the ClusterModel
   * @throws RuntimeException if serialization fails
   */
  public static ClusterModel deepCopy(ClusterModel original) {
    if (original == null) {
      return null;
    }

    long startTime = System.currentTimeMillis();

    try {
      // Serialize to byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(original);
        oos.flush();
      }

      // Deserialize from byte array
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      try (ObjectInputStream ois = new ObjectInputStream(bais)) {
        ClusterModel copy = (ClusterModel) ois.readObject();

        long duration = System.currentTimeMillis() - startTime;
        if (LOG.isDebugEnabled()) {
          LOG.debug("ClusterModel deep copy completed in {}ms (size: {} bytes)",
                    duration, baos.size());
        }

        return copy;
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to create deep copy of ClusterModel", e);
    }
  }

  /**
   * Estimates the memory footprint of a ClusterModel by serializing it.
   * Useful for monitoring and capacity planning.
   *
   * @param model The ClusterModel to measure
   * @return Size in bytes, or -1 if measurement fails
   */
  public static long estimateSize(ClusterModel model) {
    if (model == null) {
      return 0;
    }

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(model);
        oos.flush();
      }
      return baos.size();
    } catch (IOException e) {
      LOG.warn("Failed to estimate ClusterModel size", e);
      return -1;
    }
  }
}
