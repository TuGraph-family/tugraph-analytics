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
import com.antgroup.geaflow.cluster.rpc.IPipelineManagerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.IPipelineMasterEndpoint;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.rpc.proto.Container;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.baidu.brpc.client.BrpcProxy;
import com.google.protobuf.ByteString;
import java.util.concurrent.Future;

public class PipelineMasterEndpointRef extends AbstractRpcEndpointRef implements IPipelineManagerEndpointRef {

    protected IPipelineMasterEndpoint pipelineMasterEndpoint;

    public PipelineMasterEndpointRef(String host, int port,
                                     Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.pipelineMasterEndpoint = BrpcProxy.getProxy(rpcClient, IPipelineMasterEndpoint.class);
    }

    @Override
    public Future<IEvent> process(IEvent request, RpcCallback<Response> callback) {
        Container.Request taskEvent = buildRequest(request);
        this.pipelineMasterEndpoint.process(taskEvent);
        return null;
    }

    protected Request buildRequest(IEvent request) {
        ByteString payload = RpcMessageEncoder.encode(request);
        return Request.newBuilder().setPayload(payload).build();
    }
}
