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

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.driver.DriverInfo;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.master.Master;
import com.antgroup.geaflow.cluster.rpc.RpcEndpoint;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.rpc.proto.Master.ContainerIds;
import com.antgroup.geaflow.rpc.proto.Master.ContainerInfos;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.antgroup.geaflow.rpc.proto.MasterServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterEndpoint extends MasterServiceGrpc.MasterServiceImplBase implements RpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterEndpoint.class);

    private final Master master;
    private final IClusterManager clusterManager;

    public MasterEndpoint(Master master, IClusterManager clusterManager) {
        this.master = master;
        this.clusterManager = clusterManager;
    }

    @Override
    public void registerContainer(RegisterRequest request,
                                  StreamObserver<RegisterResponse> responseObserver) {
        try {
            ContainerInfo containerInfo = RpcMessageEncoder.decode(request.getPayload());
            RegisterResponse response = ((AbstractClusterManager) clusterManager)
                .registerContainer(containerInfo);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("register container failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void registerDriver(RegisterRequest request,
                               StreamObserver<RegisterResponse> responseObserver) {
        try {
            DriverInfo driverInfo = RpcMessageEncoder.decode(request.getPayload());
            RegisterResponse response = ((AbstractClusterManager) clusterManager)
                .registerDriver(driverInfo);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("register driver failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void receiveHeartbeat(HeartbeatRequest request, StreamObserver<Empty> responseObserver) {
        try {
            HeartbeatManager heartbeatManager = ((AbstractClusterManager) clusterManager).getClusterContext()
                .getHeartbeatManager();
            Heartbeat heartbeat = new Heartbeat(request.getId());
            heartbeat.setTimestamp(request.getTimestamp());
            heartbeat.setProcessMetrics(RpcMessageEncoder.decode(request.getPayload()));
            heartbeatManager.receivedHeartbeat(heartbeat);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("process {} heartbeat failed: {}", request.getId(), t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
                                 StreamObserver<Empty> responseObserver) {
        try {
            int containerId = request.getId();
            LOGGER.info("received exception from container/driver {}, {}", containerId, RpcMessageEncoder.decode(request.getPayload()));
            ((AbstractClusterManager) clusterManager).clusterFailover(containerId);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("process {} heartbeat failed: {}", request.getId(), t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void getContainerInfo(ContainerIds request,
                                 StreamObserver<ContainerInfos> responseObserver) {
        //TODO
        responseObserver.onNext(ContainerInfos.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllContainerInfos(Empty request,
                                     StreamObserver<ContainerInfos> responseObserver) {
        //TODO
        responseObserver.onNext(ContainerInfos.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void close(Empty request,
                      StreamObserver<Empty> responseObserver) {
        try {
            master.close();
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("close failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

}
