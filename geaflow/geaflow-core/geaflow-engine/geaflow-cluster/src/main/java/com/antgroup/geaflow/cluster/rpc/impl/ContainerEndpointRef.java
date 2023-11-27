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
import com.antgroup.geaflow.cluster.rpc.IAsyncContainerEndpoint;
import com.antgroup.geaflow.cluster.rpc.IContainerEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.rpc.proto.Container;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.baidu.brpc.client.BrpcProxy;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class ContainerEndpointRef extends AbstractRpcEndpointRef implements IContainerEndpointRef {

    protected IAsyncContainerEndpoint containerEndpoint;

    public ContainerEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.containerEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncContainerEndpoint.class);
    }

    @Override
    public Future<IEvent> process(IEvent request, RpcCallback<Response> callback) {
        CompletableFuture<IEvent> result = new CompletableFuture<>();
        Container.Request req = buildRequest(request);
        this.containerEndpoint.process(req, new com.baidu.brpc.client.RpcCallback<Response>() {
            @Override
            public void success(Response response) {
                if (callback != null) {
                    callback.onSuccess(response);
                }
                ByteString payload = response.getPayload();
                IEvent event;
                if (payload == ByteString.EMPTY) {
                    event = null;
                } else {
                    event = RpcMessageEncoder.decode(payload);
                }
                result.complete(event);
            }

            @Override
            public void fail(Throwable throwable) {
                callback.onFailure(throwable);
                result.completeExceptionally(throwable);
            }
        });
        return result;
    }

    @Override
    public void closeEndpoint() {
        this.containerEndpoint.close(Empty.newBuilder().build());
        super.closeEndpoint();
    }

    protected Request buildRequest(IEvent request) {
        ByteString payload = RpcMessageEncoder.encode(request);
        return Request.newBuilder().setPayload(payload).build();
    }

}
