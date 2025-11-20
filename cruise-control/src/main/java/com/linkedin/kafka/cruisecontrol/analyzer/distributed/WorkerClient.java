/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.distributed;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client for communicating with distributed optimization workers.
 * Uses HTTP-based protocol for cluster model transfer and goal optimization requests.
 */
public class WorkerClient {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerClient.class);
    private static final int DEFAULT_TIMEOUT_MS = 600000; // 10 minutes
    private static final String OPTIMIZE_ENDPOINT = "/optimize";

    private final String _workerUrl;
    private final int _timeoutMs;

    public WorkerClient(String workerUrl, int timeoutMs) {
        _workerUrl = workerUrl;
        _timeoutMs = timeoutMs;
    }

    public WorkerClient(String workerUrl) {
        this(workerUrl, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Send an optimization request to the worker asynchronously.
     *
     * @param model The cluster model to optimize
     * @param goals The goals to execute
     * @param optimizedGoals Goals that have already been optimized
     * @return A future containing the optimization result
     */
    public CompletableFuture<WorkerOptimizationResult> optimizeAsync(
            ClusterModel model,
            List<Goal> goals,
            Set<Goal> optimizedGoals) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                return optimize(model, goals, optimizedGoals);
            } catch (Exception e) {
                LOG.error("Worker optimization failed for {}", _workerUrl, e);
                throw new RuntimeException("Worker optimization failed", e);
            }
        });
    }

    /**
     * Send an optimization request to the worker synchronously.
     *
     * @param model The cluster model to optimize
     * @param goals The goals to execute
     * @param optimizedGoals Goals that have already been optimized
     * @return The optimization result
     * @throws IOException if communication fails
     */
    public WorkerOptimizationResult optimize(
            ClusterModel model,
            List<Goal> goals,
            Set<Goal> optimizedGoals) throws IOException {

        long startTime = System.currentTimeMillis();

        // Serialize request
        WorkerOptimizationRequest request = new WorkerOptimizationRequest(model, goals, optimizedGoals);
        byte[] requestBytes = serializeRequest(request);

        LOG.info("Sending optimization request to worker {} ({} bytes, {} goals)",
                _workerUrl, requestBytes.length, goals.size());

        // Send HTTP request
        URL url = new URL(_workerUrl + OPTIMIZE_ENDPOINT);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(_timeoutMs);
            conn.setReadTimeout(_timeoutMs);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));

            // Write request
            try (OutputStream out = conn.getOutputStream()) {
                out.write(requestBytes);
                out.flush();
            }

            // Read response
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Worker returned error code: " + responseCode);
            }

            byte[] responseBytes;
            try (DataInputStream in = new DataInputStream(conn.getInputStream())) {
                int length = in.readInt();
                responseBytes = new byte[length];
                in.readFully(responseBytes);
            }

            WorkerOptimizationResult result = deserializeResponse(responseBytes);

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("Worker {} completed optimization in {} ms", _workerUrl, duration);

            return result;

        } finally {
            conn.disconnect();
        }
    }

    /**
     * Check if the worker is available.
     *
     * @return true if worker responds to health check
     */
    public boolean isAvailable() {
        try {
            URL url = new URL(_workerUrl + "/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            int responseCode = conn.getResponseCode();
            conn.disconnect();
            return responseCode == HttpURLConnection.HTTP_OK;
        } catch (Exception e) {
            LOG.warn("Worker {} is not available: {}", _workerUrl, e.getMessage());
            return false;
        }
    }

    private byte[] serializeRequest(WorkerOptimizationRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            // Serialize cluster model
            ClusterModelSnapshot snapshot = ClusterModelSnapshot.create(request.getModel());
            byte[] modelBytes = snapshot.getSerializedBytes();
            dos.writeInt(modelBytes.length);
            dos.write(modelBytes);

            // Serialize goal names
            List<Goal> goals = request.getGoals();
            dos.writeInt(goals.size());
            for (Goal goal : goals) {
                writeString(dos, goal.getClass().getName());
            }

            // Serialize optimized goal names
            Set<Goal> optimizedGoals = request.getOptimizedGoals();
            dos.writeInt(optimizedGoals.size());
            for (Goal goal : optimizedGoals) {
                writeString(dos, goal.getClass().getName());
            }

            dos.flush();
        }
        return baos.toByteArray();
    }

    private WorkerOptimizationResult deserializeResponse(byte[] responseBytes) throws IOException {
        // Placeholder - actual implementation depends on result format
        return new WorkerOptimizationResult(null, null, 0);
    }

    private void writeString(DataOutputStream dos, String str) throws IOException {
        byte[] bytes = str.getBytes("UTF-8");
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public String getWorkerUrl() {
        return _workerUrl;
    }
}
