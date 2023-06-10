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

import com.antgroup.geaflow.cluster.driver.IDriver;
import com.antgroup.geaflow.cluster.rpc.RpcEndpoint;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineReq;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineRes;
import com.antgroup.geaflow.rpc.proto.DriverServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverEndpoint extends DriverServiceGrpc.DriverServiceImplBase implements RpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEndpoint.class);

    private final IDriver driver;

    public DriverEndpoint(IDriver driver) {
        this.driver = driver;
    }

    @Override
    public void executePipeline(PipelineReq request, StreamObserver<PipelineRes> responseObserver) {
        try {
            // Send sync message to driver.
            responseObserver.onNext(PipelineRes.newBuilder().build());
            Pipeline pipeline = RpcMessageEncoder
                .decode(request.getPayload());
            Object result = driver.executePipeline(pipeline);
            responseObserver.onNext(PipelineRes.newBuilder()
                .setPayload(RpcMessageEncoder.encode(result))
                .build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOGGER.error("execute pipeline failed: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    public void close(Empty request,
                      StreamObserver<Empty> responseObserver) {
        try {
            driver.close();
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("close failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }
}
