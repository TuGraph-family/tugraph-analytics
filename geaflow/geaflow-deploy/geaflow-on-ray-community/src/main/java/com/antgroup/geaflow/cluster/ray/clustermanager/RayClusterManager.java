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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.AGENT_PROFILER_PATH;
import static com.antgroup.geaflow.cluster.ray.config.RayConfig.RAY_AGENT_PROFILER_PATH;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.ray.context.RayContainerContext;
import com.antgroup.geaflow.cluster.ray.context.RayDriverContext;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayContainerRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayDriverRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RayMasterRunner;
import com.antgroup.geaflow.cluster.ray.entrypoint.RaySupervisorRunner;
import com.antgroup.geaflow.cluster.ray.utils.RaySystemFunc;
import com.antgroup.geaflow.cluster.runner.manager.GeaFlowClusterManager;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterManager extends GeaFlowClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterManager.class);
    private static final int MASTER_ACTOR_ID = 0;
    private static Map<Integer, ActorHandle> actors = new HashMap<>();

    private String classpath;
    private String autoRestart;
    private String currentJobId;

    public RayClusterManager() {
        super(EnvType.RAY_COMMUNITY);
    }

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        if (clusterContext.getHeartbeatManager() != null) {
            this.failFast = clusterConfig.getMaxRestarts() == 0;
            this.autoRestart = config.getString(PROCESS_AUTO_RESTART);
            this.classpath = buildClasspath(config.getString(JOB_WORK_PATH));
            this.currentJobId = Ray.getRuntimeContext().getCurrentJobId().toString();
        }
        String profilerPath = config.getString(RAY_AGENT_PROFILER_PATH);
        this.config.put(AGENT_PROFILER_PATH, profilerPath);
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
    public void createNewContainer(int containerId, boolean isRecover) {
        if (enableSupervisor) {
            String logFile = String.format("container-%s-%s.log", currentJobId, containerId);
            String command = getContainerShellCommand(containerId, isRecover, classpath, logFile);
            ActorHandle<RaySupervisorRunner> actor = createSupervisor(containerId, command,
                autoRestart);
            actors.put(containerId, actor);
        } else {
            ContainerContext containerContext = new RayContainerContext(containerId,
                clusterConfig.getConfig());
            ActorHandle<RayContainerRunner> container = RayClient.createContainer(clusterConfig, containerContext);
            actors.put(containerId, container);
        }

    }

    @Override
    public void createNewDriver(int driverId, int driverIndex) {
        if (enableSupervisor) {
            String logFile = String.format("driver-%s-%s.log", currentJobId, driverId);
            String command = getDriverShellCommand(driverId, driverIndex, classpath, logFile);
            ActorHandle<RaySupervisorRunner> actor = createSupervisor(driverId, command,
                autoRestart);
            actors.put(driverId, actor);
        } else {
            DriverContext driverContext = new RayDriverContext(driverId, driverIndex, clusterConfig.getConfig());
            ActorHandle<RayDriverRunner> driver = RayClient.createDriver(clusterConfig,
                driverContext);
            actors.put(driverId, driver);
        }
        LOGGER.info("call driver start, id:{} index:{}", driverId, driverIndex);
    }

    @Override
    public void restartContainer(int containerId) {
        if (!actors.containsKey(containerId)) {
            throw new GeaflowRuntimeException(String.format("invalid container id %s", containerId));
        }
        actors.get(containerId).kill();
    }

    @Override
    public void restartDriver(int driverId) {
        if (!actors.containsKey(driverId)) {
            throw new GeaflowRuntimeException(String.format("invalid driver id %s", driverId));
        }
        actors.get(driverId).kill();
    }

    private ActorHandle<RaySupervisorRunner> createSupervisor(int containerId, String command, String autoStart) {
        Map<String, String> additionalEnvs = buildSupervisorEnvs(containerId, command, autoStart);
        return RayClient.createSupervisor(clusterConfig, additionalEnvs);
    }

    private String buildClasspath(String workDir) {
        return Paths.get(workDir).resolve("*").toString();
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
