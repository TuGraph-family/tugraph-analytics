/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.shuffle.config;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CLIENT_THREADS_NUM;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_INITIAL_BACKOFF_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_MAX_BACKOFF_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_MAX_RETRIES;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CUSTOM_FRAME_DECODER_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_PREFER_DIRECT_BUFFER;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_RECEIVE_BUFFER_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SEND_BUFFER_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_BACKLOG;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_HOST;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_PORT;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_THREADS_NUM;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_THREAD_CACHE_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_CACHE_SPILL_THRESHOLD;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_COMPRESSION_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_EMIT_BUFFER_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_EMIT_QUEUE_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_QUEUE_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FLUSH_BUFFER_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_SLICE_MAX_SPILL_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_STORAGE_TYPE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_WRITE_BUFFER_SIZE_BYTES;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.shuffle.StorageLevel;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleConfig.class);

    private final int serverPort;
    private final String serverAddress;
    private final boolean threadCacheEnabled;
    // Connect timeout in milliseconds. Default 120 secs.
    private final int connectTimeoutMs;
    // fetch timeout in milliseconds. Default is 600 secs.
    private final int fetchTimeoutMs;
    private final int serverBacklog;
    private final int serverThreads;
    private final int clientThreads;
    private final long maxSpillSizePerSlice;

    private final boolean memoryPoolEnable;
    private final int fetchQueueSize;
    private final int emitQueueSize;
    private final int emitBufferSize;
    private final int writeBufferSizeBytes;
    private final int flushBufferTimeoutMs;
    private final double cacheSpillThreshold;

    private final Configuration configuration;

    private boolean compressionEnabled;

    private static ShuffleConfig INSTANCE;

    private ShuffleConfig(Configuration config) {
        this.serverAddress = config.getString(NETTY_SERVER_HOST);
        this.serverPort = config.getInteger(NETTY_SERVER_PORT);
        this.threadCacheEnabled = config.getBoolean(NETTY_THREAD_CACHE_ENABLE);

        this.connectTimeoutMs = config.getInteger(NETTY_CONNECT_TIMEOUT_MS);
        this.fetchTimeoutMs = config.getInteger(SHUFFLE_FETCH_TIMEOUT_MS);
        this.maxSpillSizePerSlice = config.getLong(SHUFFLE_SLICE_MAX_SPILL_SIZE);

        this.serverBacklog = config.getInteger(NETTY_SERVER_BACKLOG);
        this.serverThreads = config.getInteger(NETTY_SERVER_THREADS_NUM);
        this.clientThreads = config.getInteger(NETTY_CLIENT_THREADS_NUM);

        this.memoryPoolEnable = config.getBoolean(SHUFFLE_MEMORY_POOL_ENABLE);
        this.fetchQueueSize = config.getInteger(SHUFFLE_FETCH_QUEUE_SIZE);
        this.emitQueueSize = config.getInteger(SHUFFLE_EMIT_QUEUE_SIZE);
        this.emitBufferSize = config.getInteger(SHUFFLE_EMIT_BUFFER_SIZE);
        this.writeBufferSizeBytes = config.getInteger(SHUFFLE_WRITE_BUFFER_SIZE_BYTES);
        this.flushBufferTimeoutMs = config.getInteger(SHUFFLE_FLUSH_BUFFER_TIMEOUT_MS);
        this.cacheSpillThreshold = config.getDouble(SHUFFLE_CACHE_SPILL_THRESHOLD);

        this.compressionEnabled = config.getBoolean(SHUFFLE_COMPRESSION_ENABLE);

        this.configuration = config;
        LOGGER.info("init shuffle config: {}", config);
    }

    public static synchronized ShuffleConfig getInstance(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new ShuffleConfig(config);
        }
        return INSTANCE;
    }

    @VisibleForTesting
    public static synchronized void reset(Configuration config) {
        INSTANCE = new ShuffleConfig(config);
    }

    public static ShuffleConfig getInstance() {
        return INSTANCE;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

    public boolean isThreadCacheEnabled() {
        return threadCacheEnabled;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    /**
     * Requested maximum length of the queue of incoming connections.
     *
     * @return server back log.
     */
    public int getServerConnectBacklog() {
        return serverBacklog;
    }

    /** Number of threads used in the server thread pool. Default to 4, and 0 means 2x#cores. */
    public int getServerThreadsNum() {
        return serverThreads;
    }

    /** Number of threads used in the client thread pool. Default to 4, and 0 means 2x#cores. */
    public int getClientNumThreads() {
        return clientThreads;
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     * latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     * buffer size should be ~ 1.25MB
     */
    public int getReceiveBufferSize() {
        return configuration.getInteger(NETTY_RECEIVE_BUFFER_SIZE);
    }

    public int getSendBufferSize() {
        return configuration.getInteger(NETTY_SEND_BUFFER_SIZE);
    }

    public boolean preferDirectBuffer() {
        return configuration.getBoolean(NETTY_PREFER_DIRECT_BUFFER);
    }

    public boolean enableCustomFrameDecoder() {
        return configuration.getBoolean(NETTY_CUSTOM_FRAME_DECODER_ENABLE);
    }

    public int getConnectMaxRetries() {
        return configuration.getInteger(NETTY_CONNECT_MAX_RETRIES);
    }

    public int getConnectInitialBackoffMs() {
        return configuration.getInteger(NETTY_CONNECT_INITIAL_BACKOFF_MS);
    }

    public int getConnectMaxBackoffMs() {
        return configuration.getInteger(NETTY_CONNECT_MAX_BACKOFF_MS);
    }

    public boolean isMemoryPoolEnable() {
        return this.memoryPoolEnable;
    }

    public int getFetchQueueSize() {
        return this.fetchQueueSize;
    }

    public int getEmitQueueSize() {
        return this.emitQueueSize;
    }

    public int getEmitBufferSize() {
        return this.emitBufferSize;
    }

    public int getWriteBufferSizeBytes() {
        return this.writeBufferSizeBytes;
    }

    public int getFlushBufferTimeoutMs() {
        return this.flushBufferTimeoutMs;
    }

    public double getCacheSpillThreshold() {
        return this.cacheSpillThreshold;
    }

    public Configuration getConfig() {
        return configuration;
    }

    public static StorageLevel getShuffleStorageType(Configuration configuration) {
        Object confVal = configuration.getString(SHUFFLE_STORAGE_TYPE);
        if (confVal != null) {
            return StorageLevel.valueOf(String.valueOf(confVal).toLowerCase());
        }
        return StorageLevel.disk;
    }

    @Override
    public String toString() {
        return "ShuffleConfig{" + ", connectTimeoutMs=" + connectTimeoutMs + ", fetchTimeoutMs="
            + fetchTimeoutMs + ", serverBacklog=" + serverBacklog + ", serverThreads="
            + serverThreads + ", clientThreads=" + clientThreads + ", maxSpillSizePerSliceMB="
            + maxSpillSizePerSlice / FileUtils.ONE_MB + '}';
    }

}
