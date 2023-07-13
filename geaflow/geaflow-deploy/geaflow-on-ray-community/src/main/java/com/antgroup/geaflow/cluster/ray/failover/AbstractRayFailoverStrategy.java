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

package com.antgroup.geaflow.cluster.ray.failover;

import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.ray.clustermanager.RayClusterManager;
import com.antgroup.geaflow.env.IEnvironment.EnvType;

public abstract class AbstractRayFailoverStrategy implements IFailoverStrategy {

    protected RayClusterManager clusterManager;

    @Override
    public EnvType getEnv() {
        return EnvType.RAY_COMMUNITY;
    }

    public RayClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(RayClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }
}
