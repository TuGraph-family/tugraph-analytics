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
import com.antgroup.geaflow.cluster.constants.ClusterConstants;
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
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.FutureUtil;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    protected IFailoverStrategy foStrategy;
    protected Map<Integer, Future<DriverInfo>> driverFutureMap;
    protected long driverTimeoutSec;
    private AtomicInteger idGenerator;

    @Override
    public void init(ClusterContext clusterContext) {
        this.clusterConfig = clusterContext.getClusterConfig();
        this.driverTimeoutSec = clusterConfig.getDriverRegisterTimeoutSec();
        this.containerInfos = new ConcurrentHashMap<>();
        this.driverInfos = new ConcurrentHashMap<>();
        this.clusterContext = clusterContext;
        this.idGenerator = new AtomicInteger(clusterContext.getMaxComponentId());
        this.masterId = clusterContext.getConfig().getMasterId();
        this.foStrategy = buildFailoverStrategy();
        Preconditions.checkNotNull(masterId, "masterId is not set");
        this.driverFutureMap = new ConcurrentHashMap<>();
        if (clusterContext.isRecover()) {
            for (Integer driverId : clusterContext.getDriverIds().keySet()) {
                driverFutureMap.put(driverId, new CompletableFuture<>());
            }
        }
    }

    @Override
    public void allocateWorkers(int workerNum) {
        int workersPerContainer = clusterConfig.getContainerWorkerNum();
        int containerNum = (workerNum + workersPerContainer - 1) / workersPerContainer;
        LOGGER.info("allocate {} containers with {} workers", containerNum, workerNum);
        startContainers(containerNum);
        doCheckpoint();
    }

    private void startContainers(int containerNum) {
        Map<Integer, String> containerIds = new HashMap<>();
        for (int i = 0; i < containerNum; i++) {
            int containerId = generateNextComponentId();
            doStartContainer(containerId, false);
            containerIds.put(containerId, ClusterConstants.getContainerName(containerId));
        }
        clusterContext.getContainerIds().putAll(containerIds);
    }

    @Override
    public Map<String, RpcAddress> startDrivers() {
        int driverNum = clusterConfig.getDriverNum();
        if (!clusterContext.isRecover()) {
            Map<Integer, String> driverIds = new HashMap<>();
            for (int driverIndex = 0; driverIndex < driverNum; driverIndex++) {
                int driverId = generateNextComponentId();
                driverFutureMap.put(driverId, new CompletableFuture<>());
                doStartDriver(driverId, driverIndex);
                driverIds.put(driverId, ClusterConstants.getDriverName(driverId));
            }
            clusterContext.getDriverIds().putAll(driverIds);
            doCheckpoint();
        }
        Map<String, RpcAddress> driverAddresses = new HashMap<>(driverNum);
        List<DriverInfo> driverInfoList = FutureUtil
            .wait(driverFutureMap.values(), driverTimeoutSec, TimeUnit.SECONDS);
        driverInfoList.forEach(driverInfo -> driverAddresses
            .put(driverInfo.getName(), new RpcAddress(driverInfo.getHost(),
                driverInfo.getRpcPort())));
        return driverAddresses;
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        foStrategy.doFailover(componentId, cause);
    }

    @Override
    public void close() {
        if (clusterInfo != null) {
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

    private int generateNextComponentId() {
        int id = idGenerator.incrementAndGet();
        clusterContext.setMaxComponentId(id);
        return id;
    }

    public RegisterResponse registerContainer(ContainerInfo request) {
        LOGGER.info("register container:{}", request);
        containerInfos.put(request.getId(), request);
        RpcUtil.asyncExecute(() -> openContainer(request));
        return RegisterResponse.newBuilder().setSuccess(true).build();
    }

    public RegisterResponse registerDriver(DriverInfo driverInfo) {
        LOGGER.info("register driver:{}", driverInfo);
        driverInfos.put(driverInfo.getId(), driverInfo);
        CompletableFuture<DriverInfo> completableFuture =
            (CompletableFuture<DriverInfo>) driverFutureMap.get(driverInfo.getId());
        completableFuture.complete(driverInfo);
        return RegisterResponse.newBuilder().setSuccess(true).build();
    }

    protected void openContainer(ContainerInfo containerInfo) {
        ContainerEndpointRef endpointRef = RpcEndpointRefFactory.getInstance()
            .connectContainer(containerInfo.getHost(), containerInfo.getRpcPort());
        int workerNum = clusterConfig.getContainerWorkerNum();
        endpointRef.process(new OpenContainerEvent(workerNum), new RpcCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                byte[] payload = response.getPayload().toByteArray();
                OpenContainerResponseEvent openResult =
                    (OpenContainerResponseEvent) SerializerFactory
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

    private synchronized void doCheckpoint() {
        clusterContext.checkpoint(new ClusterContext.ClusterCheckpointFunction());
    }

    protected abstract IFailoverStrategy buildFailoverStrategy();

    protected abstract void doStartContainer(int containerId, boolean isRecover);

    protected abstract void doStartDriver(int driverId, int driverIndex);

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    public Map<Integer, ContainerInfo> getContainerInfos() {
        return new HashMap<>(containerInfos);
    }

    public Map<Integer, DriverInfo> getDriverInfos() {
        return new HashMap<>(driverInfos);
    }

    public Map<Integer, String> getContainerIds() {
        return clusterContext.getContainerIds();
    }

    public Map<Integer, String> getDriverIds() {
        return clusterContext.getDriverIds();
    }

}
