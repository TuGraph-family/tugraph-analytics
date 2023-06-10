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
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_COMPRESSION_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_SLICE_MAX_SPILL_SIZE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_STORAGE_TYPE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.shuffle.StorageLevel;
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
