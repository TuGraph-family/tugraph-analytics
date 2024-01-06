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

package com.antgroup.geaflow.analytics.service.client;

import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsClientConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc.AnalyticsServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcQueryExecutor extends AbstractQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcQueryExecutor.class);
    private static final int DEFAULT_SHUTDOWN_AWAIT_MS = 5;

    private final Map<HostAndPort, ManagedChannel> coordinatorAddress2Channel;
    private final Map<HostAndPort, AnalyticsServiceBlockingStub> coordinatorAddress2Stub;

    public RpcQueryExecutor(ClientHandlerContext context) {
        super(context);
        this.coordinatorAddress2Channel = new HashMap<>();
        this.coordinatorAddress2Stub = new HashMap<>();
    }

    public void initManagedChannel(HostAndPort address) {
        ManagedChannel coordinatorChannel = this.coordinatorAddress2Channel.get(address);
        if (coordinatorChannel != null && (!coordinatorChannel.isShutdown() || !coordinatorChannel.isTerminated())) {
            coordinatorChannel.shutdownNow();
            this.coordinatorAddress2Channel.put(address, null);
        }
        Throwable latestException = null;
        for (int i = 0; i < retryNum; i++) {
            try {
                ManagedChannel managedChannel = buildChannel(address.getHost(), address.getPort(), timeoutMs);
                this.coordinatorAddress2Channel.put(address, managedChannel);
                this.coordinatorAddress2Stub.put(address, AnalyticsServiceGrpc.newBlockingStub(managedChannel));
                LOGGER.info("init managed channel with address {}:{}", address.getHost(), address.getPort());
                return;
            } catch (Throwable e) {
                latestException = e;
                LOGGER.warn("init managed channel [{}:{}] failed, retry {}", address.getHost(),
                    address.getPort(), i + 1, e);
            }
        }
        String msg = String.format("try connect to [%s:%d] fail after %d times", address.getHost(), address.getPort(), this.retryNum);
        LOGGER.error(msg, latestException);
        throw new GeaflowRuntimeException(RuntimeErrors.INST.analyticsClientError(msg),
            latestException);
    }


    public void ensureChannelAlive(HostAndPort address) {
        ManagedChannel channel = this.coordinatorAddress2Channel.get(address);
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            LOGGER.warn("connection of [{}:{}] lost, reconnect...", address.getHost(), address.getPort());
            this.initManagedChannel(address);
        }
    }


    public void shutdown() {
        for (Entry<HostAndPort, ManagedChannel> entry : this.coordinatorAddress2Channel.entrySet()) {
            ManagedChannel channel = entry.getValue();
            HostAndPort address = entry.getKey();
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(DEFAULT_SHUTDOWN_AWAIT_MS, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOGGER.warn("coordinator [{}:{}] shutdown failed", address.getHost(), address.getPort(), e);
                    throw new GeaflowRuntimeException(String.format("coordinator [%s:%d] "
                        + "shutdown error", address.getHost(), address.getPort()), e);
                }
            }
        }
        LOGGER.info("shutdown all channel");
    }

    private ManagedChannel buildChannel(String host, int port, int timeoutMs) {
        return NettyChannelBuilder.forAddress(host, port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
            .maxInboundMessageSize(config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_MAX_INBOUND_MESSAGE_SIZE))
            .maxRetryAttempts(config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_MAX_RETRY_ATTEMPTS))
            .retryBufferSize(config.getLong(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_DEFALUT_RETRY_BUFFER_SIZE))
            .perRpcBufferLimit(config.getLong(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_PER_RPC_BUFFER_LIMIT))
            .usePlaintext()
            .build();
    }

    public AnalyticsServiceBlockingStub getAnalyticsServiceBlockingStub(HostAndPort address) {
        ensureChannelAlive(address);
        AnalyticsServiceBlockingStub analyticsServiceBlockingStub = this.coordinatorAddress2Stub.get(address);
        if (analyticsServiceBlockingStub != null) {
            return analyticsServiceBlockingStub;
        }
        throw new GeaflowRuntimeException(String.format("coordinator address [%s:%d] get rpc stub "
            + "fail", address.getHost(), address.getPort()));
    }

}
