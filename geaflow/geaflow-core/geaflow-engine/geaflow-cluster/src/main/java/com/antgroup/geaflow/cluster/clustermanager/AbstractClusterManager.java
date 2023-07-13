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

package com.antgroup.geaflow.cluster.clustermanager;

import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.driver.DriverInfo;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerResponseEvent;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClusterManager implements IClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusterManager.class);

    protected String masterId;
    protected ClusterConfig clusterConfig;
    protected ClusterContext clusterContext;
    protected ClusterId clusterInfo;
    protected Map<Integer, ContainerInfo> containerInfos;
    protected Map<Integer, DriverInfo> driverInfos;
    protected Set<Integer> containerIds;
    protected Set<Integer> driverIds;
    protected IFailoverStrategy foStrategy;

    protected CompletableFuture<DriverInfo> driverFuture;
    protected long driverTimeoutSec;
    private AtomicInteger idGenerator;

    @Override
    public void init(ClusterContext clusterContext) {
        this.clusterConfig = clusterContext.getClusterConfig();
        this.driverTimeoutSec = clusterConfig.getDriverRegisterTimeoutSec();
        this.containerInfos = new HashMap<>();
        this.driverInfos = new HashMap<>();
        this.clusterContext = clusterContext;
        this.idGenerator = new AtomicInteger(0);
        this.masterId = clusterContext.getConfig().getMasterId();
        this.driverFuture = new CompletableFuture<>();
        this.containerIds = new HashSet<>();
        this.driverIds = new HashSet<>();
        this.foStrategy = buildFailoverStrategy();
        Preconditions.checkNotNull(masterId, "masterId is not set");
    }

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    protected abstract IFailoverStrategy buildFailoverStrategy();

    @Override
    public void allocateWorkers(int workerNum) {
        int workersPerContainer = clusterConfig.getContainerWorkerNum();
        int containerNum = (workerNum + workersPerContainer - 1) / workersPerContainer;
        LOGGER.info("allocate {} workers in {} containers[{}]", workerNum, containerNum,
            workersPerContainer);
        startContainers(containerNum);
    }

    private void startContainers(int containerNum) {
        for (int i = 0; i < containerNum; i++) {
            int containerId = idGenerator.incrementAndGet();
            doStartContainer(containerId, false);
            this.containerIds.add(containerId);
        }
    }

    @Override
    public RpcAddress startDriver() {
        try {
            int driverId = idGenerator.incrementAndGet();
            doStartDriver(driverId);
            this.driverIds.add(driverId);
            DriverInfo driverInfo = driverFuture.get(driverTimeoutSec, TimeUnit.SECONDS);
            return new RpcAddress(driverInfo.getHost(), driverInfo.getRpcPort());
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (clusterInfo != null) {
            // close master
            LOGGER.info("close master {}", masterId);
            RpcClient.getInstance().closeMasterConnection(masterId);
        }

        for (ContainerInfo containerInfo : containerInfos.values()) {
            LOGGER.info("close container {}", containerInfo.getName());
            RpcClient.getInstance().closeContainerConnection(containerInfo.getName());
        }

        for (DriverInfo driverInfo : driverInfos.values()) {
            LOGGER.info("close driver {}", driverInfo.getName());
            RpcClient.getInstance().closeDriverConnection(driverInfo.getName());
        }
    }

    public void clusterFailover(int componentId) {
        foStrategy.doFailover(componentId);
    }

    public RegisterResponse registerContainer(ContainerInfo request) {
        ContainerInfo containerInfo = containerInfos.put(request.getId(), request);
        boolean registered = (containerInfo == null);
        RegisterResponse response = RegisterResponse.newBuilder().setSuccess(registered).build();
        RpcUtil.asyncExecute(() -> openContainer(request));
        return response;
    }

    protected void openContainer(ContainerInfo containerInfo) {
        ContainerEndpointRef endpointRef = RpcEndpointRefFactory.getInstance()
            .connectContainer(containerInfo.getHost(), containerInfo.getRpcPort());
        int workerNum = clusterConfig.getContainerWorkerNum();
        endpointRef.process(new OpenContainerEvent(workerNum), new RpcCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                byte[] payload = response.getPayload().toByteArray();
                OpenContainerResponseEvent openResult = (OpenContainerResponseEvent) SerializerFactory
                    .getKryoSerializer().deserialize(payload);
                ContainerExecutorInfo executorInfo = new ContainerExecutorInfo(containerInfo,
                    openResult.getFirstWorkerIndex(), workerNum);
                handleRegisterResponse(executorInfo, openResult, null);
            }

            @Override
            public void onFailure(Throwable t) {
                handleRegisterResponse(null, null, t);
            }
        });
    }

    public RegisterResponse registerDriver(DriverInfo driverInfo) {
        driverInfos.put(driverInfo.getId(), driverInfo);
        boolean registered = driverFuture.complete(driverInfo);
        LOGGER.info("driver is registered:{}", driverInfo);
        return RegisterResponse.newBuilder().setSuccess(registered).build();
    }

    public Map<Integer, ContainerInfo> getContainerInfos() {
        return new HashMap<>(containerInfos);
    }

    private void handleRegisterResponse(ContainerExecutorInfo executorInfo,
                                        OpenContainerResponseEvent response, Throwable e) {
        List<ExecutorRegisteredCallback> callbacks = clusterContext.getCallbacks();
        if (e != null || !response.isSuccess()) {
            for (ExecutorRegisteredCallback callback : callbacks) {
                callback.onFailure(new ExecutorRegisterException(e));
            }
        } else {
            for (ExecutorRegisteredCallback callback : callbacks) {
                callback.onSuccess(executorInfo);
            }
        }
    }

    protected abstract void doStartContainer(int containerId, boolean isRecover);

    protected abstract void doStartDriver(int driverId);

    public Set<Integer> getContainerIds() {
        return new HashSet<>(containerIds);
    }

    public Set<Integer> getDriverIds() {
        return new HashSet<>(driverIds);
    }

    public void setContainerIds(Set<Integer> containerIds) {
        this.containerIds = containerIds;
    }

    public void setDriverIds(Set<Integer> driverIds) {
        this.driverIds = driverIds;
    }
}
