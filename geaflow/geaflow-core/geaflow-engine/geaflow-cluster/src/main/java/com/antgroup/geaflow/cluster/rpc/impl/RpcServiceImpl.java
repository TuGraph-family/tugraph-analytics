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

import com.antgroup.geaflow.cluster.rpc.RpcEndpoint;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory;
import com.antgroup.geaflow.cluster.rpc.RpcService;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.Serializable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcServiceImpl implements RpcService, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServiceImpl.class);

    private int port;
    private Server server;
    private final ServerBuilder serverBuilder;

    public RpcServiceImpl(Configuration config) {
        this(0, config);
    }

    public RpcServiceImpl(int port, Configuration config) {
        this.serverBuilder = ServerBuilder.forPort(port);
        RpcEndpointRefFactory.getInstance(config);
    }

    public void addEndpoint(RpcEndpoint rpcEndpoint) {
        if (rpcEndpoint instanceof BindableService) {
            serverBuilder.addService((BindableService) rpcEndpoint);
        }
    }

    @Override
    public int startService() {
        try {
            this.server = serverBuilder.build().start();
            this.port = server.getPort();
            LOGGER.info("Server started, listening on: {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    LOGGER.warn("*** shutting down gRPC server since JVM is shutting down");
                    stopService();
                    LOGGER.warn("*** server shut down");
                }
            });
            return port;
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw new GeaflowRuntimeException(t);
        }
    }

    @Test
    public void waitTermination() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            LOGGER.warn("shutdown is interrupted");
        }
    }

    @Override
    public void stopService() {
        if (server != null) {
            server.shutdown();
        }
    }
}
