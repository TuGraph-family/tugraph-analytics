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

import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResponse;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResponse;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.resourcemanager.allocator.IAllocator;
import com.antgroup.geaflow.cluster.rpc.RpcEndpoint;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Resource;
import com.antgroup.geaflow.rpc.proto.ResourceServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManagerEndpoint extends ResourceServiceGrpc.ResourceServiceImplBase implements
    RpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerEndpoint.class);

    private final IResourceManager resourceManager;

    public ResourceManagerEndpoint(IResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    @Override
    public void requireResource(Resource.RequireResourceRequest request,
                                StreamObserver<Resource.RequireResourceResponse> responseObserver) {
        try {
            RequireResponse requireResponse = this.resourceManager
                .requireResource(convertRequireRequest(request));
            responseObserver.onNext(convertRequireResponse(requireResponse));
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("require resource failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void releaseResource(Resource.ReleaseResourceRequest request,
                                StreamObserver<Resource.ReleaseResourceResponse> responseObserver) {
        try {
            ReleaseResponse releaseResponse = this.resourceManager
                .releaseResource(convertReleaseRequest(request));
            responseObserver.onNext(convertReleaseResponse(releaseResponse));
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOGGER.error("release resource failed: {}", t.getMessage(), t);
            responseObserver.onError(t);
        }
    }

    private static RequireResourceRequest convertRequireRequest(
        Resource.RequireResourceRequest request) {
        IAllocator.AllocateStrategy strategy;
        switch (request.getAllocStrategy()) {
            case ROUND_ROBIN:
                strategy = IAllocator.AllocateStrategy.ROUND_ROBIN;
                break;
            default:
                String msg = "unrecognized allocate strategy" + request.getAllocStrategy();
                throw new GeaflowRuntimeException(RuntimeErrors.INST.resourceError(msg));
        }
        return RequireResourceRequest.build(request.getRequireId(), request.getWorkersNum(), strategy);
    }

    private static Resource.RequireResourceResponse convertRequireResponse(RequireResponse response) {
        Resource.RequireResourceResponse.Builder builder = Resource.RequireResourceResponse.newBuilder();
        boolean success = response.isSuccess();
        builder.setRequireId(response.getRequireId());
        builder.setSuccess(success);
        if (response.getMsg() != null) {
            builder.setMsg(response.getMsg());
        }
        if (!success) {
            return builder.build();
        }
        for (WorkerInfo workerInfo : response.getWorkers()) {
            Resource.Worker worker = Resource.Worker.newBuilder()
                .setHost(workerInfo.getHost())
                .setProcessId(workerInfo.getProcessId())
                .setRpcPort(workerInfo.getRpcPort())
                .setShufflePort(workerInfo.getShufflePort())
                .setWorkerId(workerInfo.getWorkerIndex())
                .setContainerId(workerInfo.getContainerName())
                .build();
            builder.addWorker(worker);
        }
        return builder.build();
    }

    private static ReleaseResourceRequest convertReleaseRequest(
        Resource.ReleaseResourceRequest request) {
        List<WorkerInfo> workerInfoList = request.getWorkerList().stream().map(
            w -> WorkerInfo.build(w.getHost(), w.getRpcPort(), w.getShufflePort(),
                w.getProcessId(), w.getWorkerId(), w.getContainerId()))
            .collect(Collectors.toList());
        return ReleaseResourceRequest.build(request.getReleaseId(), workerInfoList);
    }

    private static Resource.ReleaseResourceResponse convertReleaseResponse(
        ReleaseResponse response) {
        boolean success = response.isSuccess();
        Resource.ReleaseResourceResponse.Builder builder = Resource.ReleaseResourceResponse.newBuilder()
            .setReleaseId(response.getReleaseId())
            .setSuccess(success);
        if (!success) {
            builder.setMsg(response.getMsg());
        }
        return builder.build();
    }

}
