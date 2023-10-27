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
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.DRIVER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.MASTER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.METRIC;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.PIPELINE_MANAGER;
import static com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.RESOURCE_MANAGER;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RPC_ASYNC_THREADS;

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
import com.antgroup.geaflow.cluster.rpc.impl.MetricEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.PipelineMasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.IHAService;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);
    private static final int RPC_RETRY_EXTRA_MS = 30000;

    private static IHAService haService;
    private static RpcEndpointRefFactory refFactory;
    private static RpcClient INSTANCE;
    private static int RPC_RETRY_TIMES;
    private static int RPC_RETRY_INTERVAL_MS;

    private final ExecutorService executorService;

    private RpcClient(Configuration configuration) {
        // Ensure total retry time be longer than (heartbeat timeout + 30s).
        RPC_RETRY_INTERVAL_MS = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_INTERVAL_MS);
        int heartbeatCheckMs = configuration.getInteger(HEARTBEAT_TIMEOUT_MS);
        int minTimes = (int) Math
            .ceil((double) (heartbeatCheckMs + RPC_RETRY_EXTRA_MS) / RPC_RETRY_INTERVAL_MS);
        int retryTimes = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_TIMES);
        RPC_RETRY_TIMES = Math.max(minTimes, retryTimes);
        refFactory = RpcEndpointRefFactory.getInstance(configuration);
        haService = HAServiceFactory.getService(configuration);

        int threads = configuration.getInteger(RPC_ASYNC_THREADS);
        this.executorService = new ThreadPoolExecutor(threads, threads, Long.MAX_VALUE,
            TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
            ThreadUtil.namedThreadFactory(true, "rpc-executor"));
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

    // Master endpoint ref.
    public <T> void registerContainer(String masterId, T info, RpcCallback<RegisterResponse> callback) {
        doRpcWithRetry(() -> {
            ListenableFuture<RegisterResponse> future = connectMaster(masterId).registerContainer(info);
            handleFutureCallback(future, callback, masterId);
        }, masterId, MASTER);
    }

    public void sendHeartBeat(String masterId, Heartbeat heartbeat, RpcCallback<Empty> callback) {
        doRpcWithRetry(() -> {
            ListenableFuture<Empty> future = connectMaster(masterId).sendHeartBeat(heartbeat);
            handleFutureCallback(future, callback, masterId);
        }, masterId, MASTER);
    }

    public Empty sendException(String masterId, Integer containerId, String containerName,
                               Throwable throwable) {
        return doRpcWithRetry(() -> connectMaster(masterId)
            .sendException(containerId, containerName, throwable.getMessage()), masterId, MASTER);
    }

    // Container endpoint ref.
    public Future processContainer(String containerId, IEvent event) {
        return doRpcWithRetry(() -> {
            ListenableFuture<Response> future = connectContainer(containerId).process(event);
            return new RpcResponseFuture(future);
        }, containerId, CONTAINER);
    }

    public void processContainer(String containerId, IEvent event, RpcCallback<Response> callback) {
        doRpcWithRetry(() -> {
            ListenableFuture<Response> future = connectContainer(containerId).process(event);
            handleFutureCallback(future, callback, containerId);
        }, containerId, CONTAINER);
    }

    // Pipeline endpoint ref.
    public void processPipeline(String driverId, IEvent event) {
        doRpcWithRetry(() -> connectPipelineManager(driverId).process(event), driverId,
            PIPELINE_MANAGER);
    }

    public IPipelineResult executePipeline(String driverId, Pipeline pipeline) {
        return doRpcWithRetry(() -> connectDriver(driverId).executePipeline(pipeline), driverId,
            DRIVER);
    }

    // Resource manager endpoint ref.
    public RequireResponse requireResource(String masterId, RequireResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).requireResource(request), masterId,
            RESOURCE_MANAGER);
    }

    public ReleaseResponse releaseResource(String masterId, ReleaseResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).releaseResource(request), masterId,
            RESOURCE_MANAGER);
    }

    public ListenableFuture<MetricQueryResponse> requestMetrics(String id, MetricQueryRequest request) {
        return doRpcWithRetry(() -> connectMetricServer(id).queryMetrics(request), id, METRIC);
    }

    // Close endpoint connection.
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

    private MetricEndpointRef connectMetricServer(String id) {
        ResourceData resourceData = getResourceData(id);
        return refFactory.connectMetricServer(resourceData.getHost(), resourceData.getMetricPort());
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

    private Exception handleRpcException(String resourceId, EndpointType endpointType,
                                         Throwable t) {
        try {
            invalidateEndpointCache(resourceId, endpointType);
        } catch (Throwable e) {
            String errorMsg = String.format(
                "get resource data failed caused by %s while " + "invalidate endpoint cache: #%s",
                t.getMessage(), resourceId);
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
        refFactory.invalidateEndpointCache(resourceData.getHost(), resourceData.getRpcPort(),
            endpointType);
    }

    protected ResourceData getResourceData(String resourceId) {
        return haService.resolveResource(resourceId);
    }

    public <T> void handleFutureCallback(ListenableFuture<T> future,
                                          RpcCallback<T> callback,
                                          String resourceId) {
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable T result) {
                if (callback != null) {
                    callback.onSuccess(result);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("rpc call failed", t);
                if (callback != null) {
                    callback.onFailure(t);
                }
                if (resourceId != null) {
                    haService.invalidateResource(resourceId);
                }
            }
        }, executorService);
    }

    public ExecutorService getExecutor() {
        return executorService;
    }
}
