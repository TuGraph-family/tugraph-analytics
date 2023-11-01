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
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.server.RpcServerOptions;
import com.baidu.brpc.utils.BrpcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableServerOption {

    public static final String BRPC_IO_THREAD_NUM = "brpc.io.thread.num";
    public static final String BRPC_WORKER_THREAD_NUM = "brpc.worker.thread.num";
    public static final String BRPC_ENABLE_THREADPOOL_SHARING = "brpc.enable.threadpool.sharing";
    public static final String CONNECTION_KEEP_ALIVE_TIME_SEC =
        "brpc.connection.keep.alive.time.sec";
    public static final String RPC_BUFFER_SIZE_BYTES = "brpc.buffer.size.bytes";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableServerOption.class);

    public static RpcServerOptions build(Configuration config) {
        RpcServerOptions serverOptions = new RpcServerOptions();
        serverOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        serverOptions.setMaxTryTimes(3);

        // keepAliveTime should be at least 1 minute.
        serverOptions.setKeepAliveTime(
            Math.max(config.getInteger(CONNECTION_KEEP_ALIVE_TIME_SEC, 0), 60));

        boolean threadSharing = true;
        if (config.contains(BRPC_ENABLE_THREADPOOL_SHARING)) {
            threadSharing = Boolean.parseBoolean(config.getString(BRPC_ENABLE_THREADPOOL_SHARING));
        }
        serverOptions.setGlobalThreadPoolSharing(threadSharing);
        int threadLowerBound = Math.max(8, Runtime.getRuntime().availableProcessors());
        int brpcIoThreadNum = config.getInteger(BRPC_IO_THREAD_NUM,
            Runtime.getRuntime().availableProcessors());
        int brpcWorkerThreadNum = config.getInteger(BRPC_WORKER_THREAD_NUM,
            Runtime.getRuntime().availableProcessors());
        serverOptions.setIoThreadNum(Math.min(threadLowerBound, brpcIoThreadNum));
        serverOptions.setWorkThreadNum(Math.min(threadLowerBound, brpcWorkerThreadNum));
        serverOptions.setIoEventType(BrpcConstants.IO_EVENT_NETTY_EPOLL);
        int rpcBufferSize = config.getInteger(RPC_BUFFER_SIZE_BYTES, 256 * 1024);
        serverOptions.setSendBufferSize(rpcBufferSize);
        serverOptions.setReceiveBufferSize(rpcBufferSize);

        LOGGER.info("Server Brpc io thread nums: {}, worker thread nums: {}, buffer size: {}",
            serverOptions.getIoThreadNum(), serverOptions.getWorkThreadNum(), rpcBufferSize);

        return serverOptions;
    }
}
