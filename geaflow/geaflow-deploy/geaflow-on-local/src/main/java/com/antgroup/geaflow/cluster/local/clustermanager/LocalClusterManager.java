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

package com.antgroup.geaflow.cluster.local.clustermanager;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.local.context.LocalContainerContext;
import com.antgroup.geaflow.cluster.local.context.LocalDriverContext;
import com.antgroup.geaflow.env.IEnvironment;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterManager.class);
    private String appPath;

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.appPath = getWorkPath();
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        return FailoverStrategyFactory.loadFailoverStrategy(IEnvironment.EnvType.LOCAL, FailoverStrategyType.disable_fo.name());
    }

    @Override
    public LocalClusterId startMaster() {
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        clusterInfo = new LocalClusterId(LocalClient.createMaster(clusterConfig));
        return (LocalClusterId) clusterInfo;
    }

    @Override
    public void restartContainer(int containerId) {
        // do nothing.
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {
        ContainerContext containerContext = new LocalContainerContext(containerId,
            clusterConfig.getConfig());
        LocalClient.createContainer(clusterConfig, containerContext);
    }

    @Override
    public void doStartDriver(int driverId) {
        DriverContext driverContext = new LocalDriverContext(driverId, clusterConfig.getConfig());
        LocalClient.createDriver(clusterConfig, driverContext);
        LOGGER.info("call driver start");
    }

    @Override
    public void close() {
        super.close();
        if (appPath != null) {
            FileUtils.deleteQuietly(new File(appPath));
        }
    }

    private String getWorkPath() {
        String workPath = "/tmp/" + System.currentTimeMillis();
        try {
            FileUtils.forceMkdir(new File(workPath));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return workPath;
    }

}
