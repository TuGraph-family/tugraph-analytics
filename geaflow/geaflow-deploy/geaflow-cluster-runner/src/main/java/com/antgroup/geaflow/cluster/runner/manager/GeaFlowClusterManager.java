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

package com.antgroup.geaflow.cluster.runner.manager;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.AUTO_RESTART;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_ID;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_INDEX;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_START_COMMAND;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.IS_RECOVER;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.JOB_CONFIG;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.MASTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.LOG_DIR;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_ENABLE;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.runner.entrypoint.ContainerRunner;
import com.antgroup.geaflow.cluster.runner.entrypoint.DriverRunner;
import com.antgroup.geaflow.cluster.runner.failover.AbstractFailoverStrategy;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GeaFlowClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowClusterManager.class);
    protected boolean enableSupervisor;
    protected EnvType envType;
    protected String configValue;
    protected boolean failFast;
    protected String logDir;

    public GeaFlowClusterManager(EnvType envType) {
        this.envType = envType;
    }

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.config = clusterContext.getConfig();
        this.configValue = ClusterUtils.convertConfigToString(clusterContext.getConfig());
        this.enableSupervisor = clusterContext.getConfig().getBoolean(SUPERVISOR_ENABLE);
        this.logDir = clusterContext.getConfig().getString(LOG_DIR);
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(envType,
            clusterConfig.getConfig().getString(FO_STRATEGY));
        foStrategy.init(clusterContext);
        if (foStrategy instanceof AbstractFailoverStrategy) {
            ((AbstractFailoverStrategy) foStrategy).setClusterManager(this);
        }
        return foStrategy;
    }

    @Override
    public void restartAllDrivers() {
        Map<Integer, String> driverIds = clusterContext.getDriverIds();
        LOGGER.info("Restart all drivers: {}", driverIds);
        if (enableSupervisor) {
            restartContainersBySupervisor(driverIds, true);
        } else {
            for (Map.Entry<Integer, String> entry : driverIds.entrySet()) {
                restartDriver(entry.getKey());
            }
        }
    }

    @Override
    public void restartAllContainers() {
        Map<Integer, String> containerIds = clusterContext.getContainerIds();
        LOGGER.info("Restart all containers: {}", containerIds);
        if (enableSupervisor) {
            restartContainersBySupervisor(containerIds, false);
        } else {
            for (Map.Entry<Integer, String> entry : containerIds.entrySet()) {
                restartContainer(entry.getKey());
            }
        }
    }

    protected void restartContainersBySupervisor(Map<Integer, String> containerIds,
                                                 boolean isDriver) {
        List<Future> futures = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : containerIds.entrySet()) {
            futures.add(
                RpcClient.getInstance().restartSupervisorContainer(entry.getValue(), failFast));
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
                restartDriver(id);
            }
        } else {
            LOGGER.info("Restart lost containers: {}", lostWorkers);
            for (Integer id : lostWorkers) {
                restartContainer(id);
            }
        }
    }

    public String getDriverShellCommand(int driverId, int driverIndex, String classpath,
                                        String logFile) {
        Map<String, String> extraOptions = buildExtraOptions(driverId);
        extraOptions.put(CONTAINER_INDEX, String.valueOf(driverIndex));

        String logFilename = logDir + File.separator + logFile;
        return ClusterUtils.getStartCommand(clusterConfig.getDriverJvmOptions(), DriverRunner.class,
            logFilename, clusterConfig.getConfig(), extraOptions, classpath);
    }

    public String getContainerShellCommand(int containerId, boolean isRecover, String classpath,
                                           String logFile) {
        Map<String, String> extraOptions = buildExtraOptions(containerId);
        extraOptions.put(IS_RECOVER, String.valueOf(isRecover));

        String logFilename = logDir + File.separator + logFile;
        return ClusterUtils.getStartCommand(clusterConfig.getContainerJvmOptions(),
            ContainerRunner.class, logFilename, clusterConfig.getConfig(), extraOptions, classpath);
    }

    protected Map<String, String> buildExtraOptions(int containerId) {
        Map<String, String> env = new HashMap<>();
        env.put(MASTER_ID, masterId);
        env.put(CONTAINER_ID, String.valueOf(containerId));
        env.put(JOB_CONFIG, configValue);
        return env;
    }

    protected Map<String, String> buildSupervisorEnvs(int containerId, String startCommand,
                                                      String autoRestart) {
        Map<String, String> env = new HashMap<>();
        env.put(AUTO_RESTART, autoRestart);
        env.put(CONTAINER_ID, String.valueOf(containerId));
        env.put(CONTAINER_START_COMMAND, startCommand);
        return env;
    }

}
