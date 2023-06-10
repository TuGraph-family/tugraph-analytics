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

package com.antgroup.geaflow.cluster.ray.clustermanager;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.failover.FoStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.ray.context.RayContainerContext;
import com.antgroup.geaflow.cluster.ray.context.RayDriverContext;
import com.antgroup.geaflow.cluster.ray.utils.RaySystemFunc;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterManager.class);
    private String appPath;

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.appPath = RaySystemFunc.getWorkPath();
    }

    @Override
    protected IFailoverStrategy buildFoStrategy() {
        return FoStrategyFactory.loadFoStrategy(EnvType.RAY, clusterConfig.getConfig().getString(FO_STRATEGY));
    }

    @Override
    public RayClusterId startMaster() {
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        clusterInfo = new RayClusterId(RayClient.createMaster(clusterConfig));
        return (RayClusterId) clusterInfo;
    }

    @Override
    public void restartContainer(int containerId) {
        // do nothing.
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {
        ContainerContext containerContext = new RayContainerContext(containerId,
            clusterConfig.getConfig());
        RayClient.createContainer(clusterConfig, containerContext);
    }

    @Override
    public void doStartDriver(int driverId) {
        DriverContext driverContext = new RayDriverContext(driverId, clusterConfig.getConfig());
        RayClient.createDriver(clusterConfig, driverContext);
        LOGGER.info("call driver start");
    }

    @Override
    public void close() {
        super.close();
        if (RaySystemFunc.isLocalMode()) {
            FileUtils.deleteQuietly(new File(appPath));
        }
    }

}
