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

package com.antgroup.geaflow.common.rpc;

import com.antgroup.geaflow.common.config.Configuration;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.utils.BrpcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableClientOption {

    public static final String BRPC_CONNECTION_NUMS = "brpc.connection.nums";
    public static final String BRPC_CHANNEL_TYPE = "brpc.channel.type";
    public static final String BRPC_CONNECTION_TIMEOUT_MS = "brpc.connect.timeout.ms";
    public static final String BRPC_IO_THREAD_NUM = "brpc.io.thread.num";
    public static final String BRPC_WORKER_THREAD_NUM = "brpc.worker.thread.num";
    public static final String BRPC_ENABLE_THREADPOOL_SHARING = "brpc.enable.threadpool.sharing";
    public static final String CONNECTION_KEEP_ALIVE_TIME_SEC = "brpc.connection.keep.alive.time"
        + ".sec";
    public static final String RPC_BUFFER_SIZE_BYTES = "brpc.buffer.size.bytes";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableClientOption.class);

    public static RpcClientOptions build(Configuration config) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setMaxTryTimes(3);

        int brpcConnections = config.getInteger(BRPC_CONNECTION_NUMS, 2);
        // only works for pooled connection
        clientOption.setMaxTotalConnections(brpcConnections);
        // only works for pooled connection
        clientOption.setMinIdleConnections(brpcConnections);
        LOGGER.info("Brpc Connection nums: {}", brpcConnections);

        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

        // keepAliveTime should be at least 1 minute.
        clientOption.setKeepAliveTime(
            Math.max(config.getInteger(CONNECTION_KEEP_ALIVE_TIME_SEC, 0), 60));

        boolean threadSharing = true;
        if (config.contains(BRPC_ENABLE_THREADPOOL_SHARING)) {
            threadSharing = Boolean.parseBoolean(config.getString(BRPC_ENABLE_THREADPOOL_SHARING));
        }
        clientOption.setGlobalThreadPoolSharing(threadSharing);
        int threadLowerBound = Math.max(8, Runtime.getRuntime().availableProcessors());
        int brpcIoThreadNum = config.getInteger(BRPC_IO_THREAD_NUM,
            Runtime.getRuntime().availableProcessors());
        int brpcWorkerThreadNum = config.getInteger(BRPC_WORKER_THREAD_NUM,
            Runtime.getRuntime().availableProcessors());
        clientOption.setIoThreadNum(Math.max(threadLowerBound, brpcIoThreadNum));
        clientOption.setWorkThreadNum(Math.max(threadLowerBound, brpcWorkerThreadNum));
        clientOption.setIoEventType(BrpcConstants.IO_EVENT_NETTY_EPOLL);
        int rpcBufferSize = config.getInteger(RPC_BUFFER_SIZE_BYTES, 256 * 1024);
        clientOption.setSendBufferSize(rpcBufferSize);
        clientOption.setReceiveBufferSize(rpcBufferSize);

        LOGGER.info("Brpc io thread nums: {}, worker thread nums: {}, buffer size: {}",
            clientOption.getIoThreadNum(), clientOption.getWorkThreadNum(), rpcBufferSize);

        // SINGLE_CONNECTION is default value if not config the channel type
        int brpcChannelType = config.getInteger(BRPC_CHANNEL_TYPE, 1);
        if (0 == brpcChannelType) {
            clientOption.setChannelType(ChannelType.POOLED_CONNECTION);
        } else if (2 == brpcChannelType) {
            clientOption.setChannelType(ChannelType.SHORT_CONNECTION);
        } else {
            clientOption.setChannelType(ChannelType.SINGLE_CONNECTION);
        }

        int timeoutMillis = config.getInteger(BRPC_CONNECTION_TIMEOUT_MS, 20000);
        clientOption.setWriteTimeoutMillis(timeoutMillis);
        clientOption.setReadTimeoutMillis(timeoutMillis);
        clientOption.setConnectTimeoutMillis(timeoutMillis * 2);

        LOGGER.info("brpc time out {}ms", timeoutMillis);
        return clientOption;
    }
}
