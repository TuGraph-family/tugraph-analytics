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

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRpcEndpointRef implements RpcEndpointRef {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcEndpointRef.class);
    private static final int DEFAULT_MAX_RETRY_TIMES = 10;

    protected final String host;
    protected final int port;
    protected final int timeoutMs;
    protected ManagedChannel channel;

    public AbstractRpcEndpointRef(String host, int port, Configuration configuration) {
        this.host = host;
        this.port = port;
        this.timeoutMs = configuration.getInteger(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS);
        this.channel = buildChannel(host, port, timeoutMs);
        createStub(channel);
    }

    protected ManagedChannel buildChannel(String host, int port, int timeoutMs) {
        return NettyChannelBuilder.forAddress(host, port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
            .enableRetry()
            .maxRetryAttempts(DEFAULT_MAX_RETRY_TIMES)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext().build();
    }

    protected synchronized void ensureChannelAlive() {
        if (channel.isShutdown() || channel.isTerminated()) {
            channel = buildChannel(host, port, timeoutMs);
            createStub(channel);
        }
    }

    protected abstract void createStub(ManagedChannel channel);

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("close is interrupted:{}", e.getMessage());
        }
    }

}
