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
import com.antgroup.geaflow.cluster.rpc.IDriverEndpointRef;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineReq;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineRes;
import com.antgroup.geaflow.rpc.proto.DriverServiceGrpc;
import com.antgroup.geaflow.rpc.proto.DriverServiceGrpc.DriverServiceFutureStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverEndpointRef extends AbstractRpcEndpointRef implements IDriverEndpointRef {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEndpointRef.class);

    private DriverServiceFutureStub stub;
    private DriverServiceGrpc.DriverServiceBlockingStub blockingStub;

    public DriverEndpointRef(String host, int port, ExecutorService executorService) {
        super(host, port, executorService);
    }

    @Override
    protected void createStub(ManagedChannel channel) {
        this.stub = DriverServiceGrpc.newFutureStub(channel);
        this.blockingStub = DriverServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public IPipelineResult executePipeline(Pipeline pipeline) {
        ensureChannelAlive();
        LOGGER.info("send pipeline to driver, driver host:{}, port:{}. {}", super.host, super.port,
            pipeline);
        ByteString payload = RpcMessageEncoder.encode(pipeline);
        Iterator<PipelineRes> iterator =
            blockingStub.executePipeline(PipelineReq.newBuilder().setPayload(payload).build());

        // Make sure that ack response is received.
        if (iterator.hasNext()) {
            iterator.next();
        } else {
            throw new GeaflowRuntimeException("not found ack response");
        }
        return new PipelineResult(iterator);
    }

    @Override
    public void close() {
        ensureChannelAlive();
        blockingStub.close(Empty.newBuilder().build());
        super.close();
    }
}
