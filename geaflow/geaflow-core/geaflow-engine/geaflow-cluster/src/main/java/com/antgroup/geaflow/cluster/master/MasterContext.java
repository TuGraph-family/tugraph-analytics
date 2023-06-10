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

package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import java.io.Serializable;

public class MasterContext implements Serializable {

    private boolean isRecover;
    private Configuration configuration;
    private IClusterManager clusterManager;
    private EnvType envType;

    public MasterContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public MasterContext(Configuration configuration, IClusterManager clusterManager) {
        this.configuration = configuration;
        this.clusterManager = clusterManager;
    }

    public boolean isRecover() {
        return isRecover;
    }

    public void setRecover(boolean recover) {
        isRecover = recover;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public IClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public EnvType getEnvType() {
        return envType;
    }

    public void setEnvType(EnvType envType) {
        this.envType = envType;
    }
}
