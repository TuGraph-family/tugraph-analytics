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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;

import com.antgroup.geaflow.cluster.common.IReliableContext;
import com.antgroup.geaflow.cluster.common.ReliableContainerContext;
import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterContext extends ReliableContainerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterContext.class);

    private final Configuration config;
    private final ClusterConfig clusterConfig;
    private final List<ExecutorRegisteredCallback> callbacks;
    private HeartbeatManager heartbeatManager;
    private Map<Integer, String> containerIds;
    private Map<Integer, String> driverIds;
    private int maxComponentId;

    public ClusterContext(Configuration configuration) {
        super(DEFAULT_MASTER_ID, configuration);
        this.config = configuration;
        this.clusterConfig = ClusterConfig.build(configuration);
        this.callbacks = new ArrayList<>();
    }

    public Configuration getConfig() {
        return config;
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public void addExecutorRegisteredCallback(ExecutorRegisteredCallback callback) {
        this.callbacks.add(callback);
    }

    public List<ExecutorRegisteredCallback> getCallbacks() {
        return callbacks;
    }

    public HeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public void setHeartbeatManager(HeartbeatManager heartbeatManager) {
        this.heartbeatManager = heartbeatManager;
    }

    public Map<Integer, String> getContainerIds() {
        return containerIds;
    }

    public void setContainerIds(Map<Integer, String> containerIds) {
        this.containerIds = containerIds;
    }

    public Map<Integer, String> getDriverIds() {
        return driverIds;
    }

    public void setDriverIds(Map<Integer, String> driverIds) {
        this.driverIds = driverIds;
    }

    public int getMaxComponentId() {
        return maxComponentId;
    }

    public void setMaxComponentId(int maxComponentId) {
        this.maxComponentId = maxComponentId;
    }

    @Override
    public void load() {
        ClusterMetaStore metaStore = ClusterMetaStore.getInstance(id, config);
        Map<Integer, String> drivers = metaStore.getDriverIds();
        Map<Integer, String> containerIds = metaStore.getContainerIds();
        if (drivers != null && !drivers.isEmpty() && containerIds != null && !containerIds
            .isEmpty()) {
            this.isRecover = true;
            this.driverIds = drivers;
            this.containerIds = containerIds;
            this.maxComponentId = metaStore.getMaxContainerId();
            LOGGER.info("recover {} containers and {} drivers maxComponentId {} from metaStore",
                containerIds.size(), drivers.size(), maxComponentId);
        } else {
            this.isRecover = false;
            this.driverIds = new ConcurrentHashMap<>();
            this.containerIds = new ConcurrentHashMap<>();
            this.maxComponentId = 0;
        }
    }

    public void setRecover(boolean isRecovered) {
        this.isRecover = isRecovered;
    }

    public static class ClusterCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            ClusterContext clusterContext = (ClusterContext) context;
            Map<Integer, String> containerIds = clusterContext.getContainerIds();
            Map<Integer, String> driverIds = clusterContext.getDriverIds();
            ClusterMetaStore metaStore = ClusterMetaStore
                .getInstance(clusterContext.id, clusterContext.config);
            if (containerIds != null && !containerIds.isEmpty() && driverIds != null && !driverIds
                .isEmpty()) {
                LOGGER.info("persist {} containers and {} drivers into metaStore",
                    containerIds.size(), driverIds.size());
                metaStore.saveMaxContainerId(clusterContext.getMaxComponentId());
                metaStore.saveContainerIds(containerIds);
                metaStore.saveDriverIds(driverIds);
                metaStore.flush();
            }
        }
    }

}
