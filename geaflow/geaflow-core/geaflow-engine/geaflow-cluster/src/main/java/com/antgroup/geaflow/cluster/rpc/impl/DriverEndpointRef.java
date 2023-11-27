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

import com.antgroup.geaflow.cluster.client.PipelineResult;
import com.antgroup.geaflow.cluster.rpc.IAsyncDriverEndpoint;
import com.antgroup.geaflow.cluster.rpc.IDriverEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineReq;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineRes;
import com.baidu.brpc.client.BrpcProxy;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverEndpointRef extends AbstractRpcEndpointRef implements IDriverEndpointRef {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEndpointRef.class);

    private IAsyncDriverEndpoint driverEndpoint;

    public DriverEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.driverEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncDriverEndpoint.class);
    }

    @Override
    public IPipelineResult executePipeline(Pipeline pipeline) {
        LOGGER.info("send pipeline to driver, driver host:{}, port:{}. {}", super.host, super.port, pipeline);
        ByteString payload = RpcMessageEncoder.encode(pipeline);
        PipelineReq req = PipelineReq.newBuilder().setPayload(payload).build();
        CompletableFuture<PipelineRes> result = new CompletableFuture<>();
        com.baidu.brpc.client.RpcCallback<PipelineRes> rpcCallback = RpcUtil.buildRpcCallback(null, result);
        this.driverEndpoint.executePipeline(req, rpcCallback);
        return new PipelineResult(result);
    }

    @Override
    public void closeEndpoint() {
        this.driverEndpoint.close(Empty.newBuilder().build());
        super.closeEndpoint();
    }
}
