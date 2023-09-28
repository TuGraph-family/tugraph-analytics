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

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.IContainerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcResponseFuture;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.antgroup.geaflow.rpc.proto.ContainerServiceGrpc;
import com.antgroup.geaflow.rpc.proto.ContainerServiceGrpc.ContainerServiceBlockingStub;
import com.antgroup.geaflow.rpc.proto.ContainerServiceGrpc.ContainerServiceFutureStub;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ContainerEndpointRef extends AbstractRpcEndpointRef implements IContainerEndpointRef {

    protected ContainerServiceFutureStub stub;
    protected ContainerServiceBlockingStub blockingStub;

    public ContainerEndpointRef(String host, int port, ExecutorService executorService) {
        super(host, port, executorService);
    }

    @Override
    protected void createStub(ManagedChannel channel) {
        this.stub = ContainerServiceGrpc.newFutureStub(channel);
        this.blockingStub = ContainerServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public Future<IEvent> process(IEvent request) {
        ensureChannelAlive();
        Request req = buildRequest(request);
        ListenableFuture<Response> future = stub.process(req);
        return new RpcResponseFuture(future);
    }

    @Override
    public void process(IEvent request, RpcCallback<Response> callback) {
        ensureChannelAlive();
        Request req = buildRequest(request);
        ListenableFuture<Response> future = stub.process(req);
        handleFutureCallback(future, callback);
    }

    @Override
    public void close() {
        ensureChannelAlive();
        blockingStub.close(Empty.newBuilder().build());
        super.close();
    }

    protected Request buildRequest(IEvent request) {
        ByteString payload = RpcMessageEncoder.encode(request);
        return Request.newBuilder().setPayload(payload).build();
    }

}
