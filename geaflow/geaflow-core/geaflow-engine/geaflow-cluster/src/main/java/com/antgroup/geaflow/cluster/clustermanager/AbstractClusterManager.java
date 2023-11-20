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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        RpcClient.init(clusterContext.getConfig());
    }

    @Override
    public void allocateWorkers(int workerNum) {
        int workersPerContainer = clusterConfig.getContainerWorkerNum();
        int containerNum = (workerNum + workersPerContainer - 1) / workersPerContainer;
        LOGGER.info("allocate {} containers with {} workers", containerNum, workerNum);
        startContainers(containerNum);
        doCheckpoint();
    }

    protected void startContainers(int containerNum) {
        Map<Integer, String> containerIds = new HashMap<>();
        for (int i = 0; i < containerNum; i++) {
            int containerId = generateNextComponentId();
            createNewContainer(containerId, false);
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
                createNewDriver(driverId, driverIndex);
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

    /**
     * Restart all driver processes and rebuild pod if needed.
     */
    public void restartAllDrivers() {
        Map<Integer, String> driverIds = clusterContext.getDriverIds();
        LOGGER.info("Restart all drivers: {}", driverIds);
        restartSupervisorWorkerProcess(driverIds, true);
    }

    /**
     * Restart all container processes and rebuild pod if needed.
     */
    public void restartAllContainers() {
        Map<Integer, String> containerIds = clusterContext.getContainerIds();
        LOGGER.info("Restart all containers: {}", containerIds);
        restartSupervisorWorkerProcess(containerIds, false);
    }

    private void restartSupervisorWorkerProcess(Map<Integer, String> containerIds, boolean isDriver) {
        List<Future> futures = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : containerIds.entrySet()) {
            futures.add(RpcClient.getInstance().restartSupervisorWorkerProcess(entry.getValue()));
        }
        Iterator<Entry<Integer, String>> iterator = containerIds.entrySet().iterator();
        List<Integer> lostWorkers = new ArrayList<>();
        for (Future future : futures) {
            Entry<Integer, String> entry = iterator.next();
            try {
                future.get();
            } catch (Throwable e) {
                LOGGER.warn("catch exception from {}: {} {}", entry.getValue(),
                    e.getClass().getCanonicalName(), e.getMessage());
                lostWorkers.add(entry.getKey());
            }
        }
        if (isDriver) {
            LOGGER.info("Restart lost drivers: {}", lostWorkers);
            for (Integer id : lostWorkers) {
                recreateDriver(id);
            }
        } else {
            LOGGER.info("Restart lost containers: {}", lostWorkers);
            for (Integer id : lostWorkers) {
                recreateContainer(id);
            }
        }
    }

    /**
     * Create a new driver component.
     */
    protected abstract void createNewDriver(int driverId, int index);

    /**
     * Create a new container component.
     */
    protected abstract void createNewContainer(int containerId, boolean isRecover);

    /**
     * Recreate a driver component.
     */
    public abstract void recreateDriver(int driverId);

    /**
     * Recreate a container component.
     */
    public abstract void recreateContainer(int containerId);

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

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    public int getTotalContainers() {
        return clusterContext.getContainerIds().size();
    }

    public int getTotalDrivers() {
        return clusterContext.getDriverIds().size();
    }

    public Map<Integer, ContainerInfo> getContainerInfos() {
        return containerInfos;
    }

    public Map<Integer, DriverInfo> getDriverInfos() {
        return driverInfos;
    }

    public Map<Integer, String> getContainerIds() {
        return clusterContext.getContainerIds();
    }

    public Map<Integer, String> getDriverIds() {
        return clusterContext.getDriverIds();
    }

}
