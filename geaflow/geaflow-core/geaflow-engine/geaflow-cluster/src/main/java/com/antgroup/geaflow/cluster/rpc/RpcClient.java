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

package com.antgroup.geaflow.cluster.rpc;

import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.CONTAINER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.MASTER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.PIPELINE_MANAGER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.RESOURCE_MANAGER;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_TIMEOUT_MS;

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResponse;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResponse;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType;
import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.PipelineMasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.IHAService;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    private static IHAService haService;
    private static RpcEndpointRefFactory refFactory;
    private static RpcClient INSTANCE;
    private static int RPC_RETRY_TIMES;
    private static int RPC_RETRY_INTERVAL_MS;
    private static int RPC_RETRY_EXTRA_MS = 30000;

    private RpcClient(Configuration configuration) {
        // ensure total retry time be longer than (heartbeat timeout + 30s).
        RPC_RETRY_INTERVAL_MS = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_INTERVAL_MS);
        int heartbeatCheckMs = configuration.getInteger(HEARTBEAT_TIMEOUT_MS);
        int minTimes = (int) Math.ceil((double) (heartbeatCheckMs + RPC_RETRY_EXTRA_MS) / RPC_RETRY_INTERVAL_MS);
        int retryTimes = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_TIMES);
        RPC_RETRY_TIMES = Math.max(minTimes, retryTimes);
        refFactory = RpcEndpointRefFactory.getInstance(configuration);
        haService = HAServiceFactory.getService(configuration);
    }

    public static synchronized RpcClient init(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new RpcClient(configuration);
        }
        return INSTANCE;
    }

    public static synchronized RpcClient getInstance() {
        return INSTANCE;
    }

    // master endpoint ref
    public <T> void registerContainer(String masterId, T info, RpcCallback<RegisterResponse> listener) {
        doRpcWithRetry(() -> connectMaster(masterId).registerContainer(info, listener), masterId, MASTER);
    }

    public ListenableFuture<Empty> sendHeartBeat(String masterId, Heartbeat heartbeat) {
        return doRpcWithRetry(() -> connectMaster(masterId).sendHeartBeat(heartbeat), masterId, MASTER);
    }

    public Empty sendException(String masterId, Integer containerId,
                                                 Throwable throwable) {
        return doRpcWithRetry(() -> connectMaster(masterId).sendException(containerId, throwable.getMessage()), masterId, MASTER);
    }

    // container endpoint ref
    public Future processContainer(String containerId, IEvent event) {
        return doRpcWithRetry(() -> connectContainer(containerId).process(event), containerId, CONTAINER);
    }

    public void processContainer(String containerId, IEvent event, RpcCallback<Response> callback) {
        doRpcWithRetry(() -> connectContainer(containerId).process(event, callback), containerId, CONTAINER);
    }

    // pipeline endpoint ref
    public void processPipeline(String driverId, IEvent event) {
        doRpcWithRetry(() -> connectPipelineManager(driverId).process(event), driverId, PIPELINE_MANAGER);
    }

    public void processPipeline(String driverId, IEvent event, RpcCallback<Response> callback) {
        doRpcWithRetry(() -> connectPipelineManager(driverId).process(event, callback), driverId, PIPELINE_MANAGER);
    }

    // resource manager endpoint ref
    public RequireResponse requireResource(String masterId, RequireResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).requireResource(request), masterId, RESOURCE_MANAGER);
    }

    public ReleaseResponse releaseResource(String masterId, ReleaseResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).releaseResource(request), masterId, RESOURCE_MANAGER);
    }

    // close endpoint connection
    public void closeMasterConnection(String masterId) {
        connectMaster(masterId).close();
    }

    public void closeDriverConnection(String driverId) {
        connectDriver(driverId).close();
    }

    public void closeContainerConnection(String containerId) {
        connectContainer(containerId).close();
    }

    private MasterEndpointRef connectMaster(String masterId) {
        ResourceData resourceData = getResourceData(masterId);
        return refFactory.connectMaster(resourceData.getHost(), resourceData.getRpcPort());
    }

    private ResourceManagerEndpointRef connectRM(String masterId) {
        ResourceData resourceData = getResourceData(masterId);
        return refFactory.connectResourceManager(resourceData.getHost(), resourceData.getRpcPort());
    }

    private DriverEndpointRef connectDriver(String driverId) {
        ResourceData resourceData = getResourceData(driverId);
        return refFactory.connectDriver(resourceData.getHost(), resourceData.getRpcPort());
    }

    private ContainerEndpointRef connectContainer(String containerId) {
        ResourceData resourceData = getResourceData(containerId);
        return refFactory.connectContainer(resourceData.getHost(), resourceData.getRpcPort());
    }

    private PipelineMasterEndpointRef connectPipelineManager(String id) {
        ResourceData resourceData = getResourceData(id);
        return refFactory.connectPipelineManager(resourceData.getHost(), resourceData.getRpcPort());
    }

    private <T> T doRpcWithRetry(Callable<T> function, String resourceId,
                                 EndpointType endpointType) {
        return RetryCommand.run(() -> {
            try {
                return function.call();
            } catch (Throwable t) {
                throw handleRpcException(resourceId, endpointType, t);
            }
        }, RPC_RETRY_TIMES, RPC_RETRY_INTERVAL_MS);
    }

    private void doRpcWithRetry(Runnable function, String resourceId, EndpointType endpointType) {
        RetryCommand.run(() -> {
            try {
                function.run();
            } catch (Throwable t) {
                throw handleRpcException(resourceId, endpointType, t);
            }
            return null;
        }, RPC_RETRY_TIMES, RPC_RETRY_INTERVAL_MS);
    }

    private Exception handleRpcException(String resourceId, EndpointType endpointType, Throwable t) {
        try {
            invalidateEndpointCache(resourceId, endpointType);
        } catch (Throwable e) {
            String errorMsg = String.format("get resource data failed caused by %s while "
                + "invalidate endpoint cache: #%s", t.getMessage(), resourceId);
            return new GeaflowRuntimeException(errorMsg, e);
        }
        invalidateResourceData(resourceId);
        return new GeaflowRuntimeException(String.format("do rpc failed. %s", t.getMessage()), t);
    }

    protected void invalidateResourceData(String resourceId) {
        LOGGER.info("invalidate rpc resource cache of : #{}", resourceId);
        haService.invalidateResource(resourceId);
    }

    protected void invalidateEndpointCache(String resourceId, EndpointType endpointType) {
        ResourceData resourceData = getResourceData(resourceId);
        refFactory.invalidateEndpointCache(resourceData.getHost(), resourceData.getRpcPort(), endpointType);
    }

    protected ResourceData getResourceData(String resourceId) {
        return haService.resolveResource(resourceId);
    }
}
