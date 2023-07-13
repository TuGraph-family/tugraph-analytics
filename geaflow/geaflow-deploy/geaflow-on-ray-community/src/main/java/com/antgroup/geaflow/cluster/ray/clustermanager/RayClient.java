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

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayContainerRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayDriverRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayMasterRunner;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
import java.io.Serializable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RayClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClient.class);

    public static ActorHandle<RayMasterRunner> createMaster(ClusterConfig clusterConfig) {
        int totalMemoryMb = clusterConfig.getMasterMemoryMB();
        List<String> jvmOptions = clusterConfig.getMasterJvmOptions().getJvmOptions();

        ActorHandle<RayMasterRunner> masterRayActor = Ray
            .actor(RayMasterRunner::new, clusterConfig.getConfig())
            .setMaxRestarts(clusterConfig.getMaxRestarts())
            .setLifetime(ActorLifetime.DETACHED)
            .setJvmOptions(jvmOptions).remote();
        LOGGER.info("master actor:{}, memoryMB:{}, jvmOptions:{}, isRestartAllFo:{}, foRestartTimes:{}",
            masterRayActor.getId().toString(), totalMemoryMb, jvmOptions,
            clusterConfig.isFoEnable(), clusterConfig.getMaxRestarts());
        return masterRayActor;
    }

    public static ClusterInfo initMaster(ActorHandle<RayMasterRunner> masterActor) {
        LOGGER.info("init master:{}", masterActor.getId().toString());
        ObjectRef<ClusterInfo> masterMetaRayObject = masterActor.task(RayMasterRunner::init)
            .remote();
        return masterMetaRayObject.get();
    }

    public static ActorHandle<RayDriverRunner> createDriver(ClusterConfig clusterConfig,
                                                            DriverContext context) {
        int totalMemoryMb = clusterConfig.getDriverMemoryMB();
        List<String> jvmOptions = clusterConfig.getDriverJvmOptions().getJvmOptions();

        ActorHandle<RayDriverRunner> driverRayActor = Ray
            .actor(RayDriverRunner::new, context)
            .setMaxRestarts(clusterConfig.getMaxRestarts())
            .setLifetime(ActorLifetime.DETACHED)
            .setJvmOptions(jvmOptions).remote();
        LOGGER.info("driver actor:{}, memoryMB:{}, jvmOptions:{}, isRestartAllFo:{}, foRestartTimes:{}",
            driverRayActor.getId().toString(), totalMemoryMb, jvmOptions,
            clusterConfig.isFoEnable(), clusterConfig.getMaxRestarts());
        return driverRayActor;
    }

    public static ActorHandle<RayContainerRunner> createContainer(ClusterConfig clusterConfig,
                                                                  ContainerContext containerContext) {
        ActorHandle<RayContainerRunner> rayContainer = Ray
            .actor(RayContainerRunner::new, containerContext)
            .setMaxRestarts(clusterConfig.getMaxRestarts())
            .setLifetime(ActorLifetime.DETACHED)
            .remote();
        LOGGER.info("worker actor {} isDisableFo {}", rayContainer.getId().toString(),
            clusterConfig.isFoEnable());
        return rayContainer;
    }

}
