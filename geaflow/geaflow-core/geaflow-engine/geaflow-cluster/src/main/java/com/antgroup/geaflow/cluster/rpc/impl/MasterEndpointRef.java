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
import com.antgroup.geaflow.cluster.rpc.IMasterEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.antgroup.geaflow.rpc.proto.MasterServiceGrpc;
import com.antgroup.geaflow.rpc.proto.MasterServiceGrpc.MasterServiceFutureStub;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;

public class MasterEndpointRef extends AbstractRpcEndpointRef implements IMasterEndpointRef {

    private MasterServiceFutureStub stub;
    private MasterServiceGrpc.MasterServiceBlockingStub blockingStub;

    public MasterEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void createStub(ManagedChannel channel) {
        this.stub = MasterServiceGrpc.newFutureStub(channel);
        this.blockingStub = MasterServiceGrpc.newBlockingStub(channel);
    }

    public <T> ListenableFuture<RegisterResponse> registerContainer(T info) {
        ensureChannelAlive();
        ByteString payload = RpcMessageEncoder.encode(info);
        RegisterRequest.Builder register = RegisterRequest.newBuilder().setPayload(payload);
        ListenableFuture<RegisterResponse> future;
        if (info instanceof DriverInfo) {
            future = stub.registerContainer(register.setIsDriver(true).build());
        } else {
            future = stub.registerContainer(register.setIsDriver(false).build());
        }
        return future;
    }

    @Override
    public ListenableFuture<Empty> sendHeartBeat(Heartbeat heartbeat) {
        ensureChannelAlive();
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(heartbeat.getContainerId())
            .setTimestamp(heartbeat.getTimestamp())
            .setName(RpcMessageEncoder.encode(heartbeat.getContainerName()))
            .setPayload(RpcMessageEncoder.encode(heartbeat.getProcessMetrics()))
            .build();
        return stub.receiveHeartbeat(heartbeatRequest);
    }

    @Override
    public Empty sendException(Integer containerId, String containerName, String message) {
        ensureChannelAlive();
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(containerId)
            .setName(RpcMessageEncoder.encode(containerName))
            .setPayload(RpcMessageEncoder.encode(message))
            .build();
        return blockingStub.receiveException(heartbeatRequest);
    }

    @Override
    public void close() {
        ensureChannelAlive();
        stub.close(Empty.newBuilder().build());
        super.close();
    }
}
