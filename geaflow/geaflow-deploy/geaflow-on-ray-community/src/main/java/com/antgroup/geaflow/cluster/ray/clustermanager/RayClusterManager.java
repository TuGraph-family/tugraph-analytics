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
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.ray.context.RayContainerContext;
import com.antgroup.geaflow.cluster.ray.context.RayDriverContext;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayContainerRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayDriverRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayMasterRunner;
import com.antgroup.geaflow.cluster.ray.failover.AbstractRayFailoverStrategy;
import com.antgroup.geaflow.cluster.ray.utils.RaySystemFunc;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterManager.class);
    private static final int MASTER_ACTOR_ID = 0;
    private static Map<Integer, ActorHandle> actors = new HashMap<>();

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(
            EnvType.RAY_COMMUNITY, clusterConfig.getConfig().getString(FO_STRATEGY));
        foStrategy.init(clusterContext);
        ((AbstractRayFailoverStrategy) foStrategy).setClusterManager(this);
        return foStrategy;
    }

    @Override
    public RayClusterId startMaster() {
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        ActorHandle<RayMasterRunner> master = RayClient.createMaster(clusterConfig);
        clusterInfo = new RayClusterId(master);
        actors.put(MASTER_ACTOR_ID, master);
        return (RayClusterId) clusterInfo;
    }

    @Override
    public void restartContainer(int containerId) {
        if (!actors.containsKey(containerId)) {
            throw new GeaflowRuntimeException(String.format("invalid container id %s", containerId));
        }
        actors.get(containerId).kill();
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {
        ContainerContext containerContext = new RayContainerContext(containerId,
            clusterConfig.getConfig());
        ActorHandle<RayContainerRunner> container = RayClient.createContainer(clusterConfig, containerContext);
        actors.put(containerId, container);
    }

    @Override
    public void doStartDriver(int driverId) {
        DriverContext driverContext = new RayDriverContext(driverId, clusterConfig.getConfig());
        ActorHandle<RayDriverRunner> driver = RayClient.createDriver(clusterConfig, driverContext);
        actors.put(driverId, driver);
        LOGGER.info("call driver start");
    }

    @Override
    public void close() {
        super.close();
        actors.clear();
        if (RaySystemFunc.isLocalMode()) {
            FileUtils.deleteQuietly(new File(RaySystemFunc.getWorkPath()));
        }
    }

}
