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
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.common.config.Configuration;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ClusterContext implements Serializable {

    private final Configuration config;
    private final ClusterConfig clusterConfig;
    private final List<ExecutorRegisteredCallback> callbacks;
    private HeartbeatManager heartbeatManager;

    public ClusterContext(Configuration configuration) {
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

}
