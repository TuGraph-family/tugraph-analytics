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

import com.antgroup.geaflow.cluster.driver.DriverInfo;
import com.antgroup.geaflow.cluster.rpc.IAsyncMasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.IMasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatResponse;
import com.antgroup.geaflow.rpc.proto.Master.RegisterRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.baidu.brpc.client.BrpcProxy;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class MasterEndpointRef extends AbstractRpcEndpointRef implements IMasterEndpointRef {

    private IAsyncMasterEndpoint masterEndpoint;

    public MasterEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.masterEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncMasterEndpoint.class);
    }

    public <T> Future<RegisterResponse> registerContainer(T info, RpcEndpointRef.RpcCallback<RegisterResponse> callback) {
        CompletableFuture<RegisterResponse> result = new CompletableFuture<>();
        ByteString payload = RpcMessageEncoder.encode(info);
        RegisterRequest register = RegisterRequest.newBuilder().setPayload(payload).build();
        com.baidu.brpc.client.RpcCallback<RegisterResponse> rpcCallback = RpcUtil.buildRpcCallback(callback, result);
        if (info instanceof DriverInfo) {
            this.masterEndpoint.registerDriver(register, rpcCallback);
        } else {
            this.masterEndpoint.registerContainer(register, rpcCallback);
        }
        return result;
    }

    @Override
    public Future<HeartbeatResponse> sendHeartBeat(Heartbeat heartbeat, RpcCallback<HeartbeatResponse> callback) {
        CompletableFuture<HeartbeatResponse> result = new CompletableFuture<>();
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(heartbeat.getContainerId())
            .setTimestamp(heartbeat.getTimestamp())
            .setName(RpcMessageEncoder.encode(heartbeat.getContainerName()))
            .setPayload(RpcMessageEncoder.encode(heartbeat.getProcessMetrics()))
            .build();
        com.baidu.brpc.client.RpcCallback<HeartbeatResponse> rpcCallback = RpcUtil.buildRpcCallback(callback, result);
        this.masterEndpoint.receiveHeartbeat(heartbeatRequest, rpcCallback);
        return result;
    }

    @Override
    public Empty sendException(Integer containerId, String containerName, String message) {
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(containerId)
            .setName(RpcMessageEncoder.encode(containerName))
            .setPayload(RpcMessageEncoder.encode(message))
            .build();
        return masterEndpoint.receiveException(heartbeatRequest);
    }

    @Override
    public void closeEndpoint() {
        this.masterEndpoint.close(Empty.newBuilder().build());
        super.closeEndpoint();
    }
}
