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

package com.antgroup.geaflow.cluster.runner.entrypoint;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_ID;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_INDEX;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.ENV_AGENT_PORT;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.ENV_SUPERVISOR_PORT;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.AGENT_HTTP_PORT;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_RPC_PORT;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import com.antgroup.geaflow.cluster.constants.ClusterConstants;
import com.antgroup.geaflow.cluster.driver.Driver;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.cluster.runner.util.RunnerRuntimeHook;
import com.antgroup.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverRunner.class);
    private final int driverId;
    private final int driverIndex;
    private final Configuration config;
    private Driver driver;

    public DriverRunner(int driverId, int driverIndex, Configuration config) {
        this.driverId = driverId;
        this.driverIndex = driverIndex;
        this.config = config;
    }

    public void run() {
        DriverContext driverContext = new DriverContext(driverId, driverIndex, config);
        int rpcPort = config.getInteger(DRIVER_RPC_PORT);
        driver = new Driver(rpcPort);
        driverContext.load();
        driver.init(driverContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        driver.waitTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            final long startTime = System.currentTimeMillis();

            String id = ClusterUtils.getProperty(CONTAINER_ID);
            String index = ClusterUtils.getProperty(CONTAINER_INDEX);
            String masterId = ClusterUtils.getProperty(ClusterConstants.MASTER_ID);
            LOGGER.info("ResourceID assigned for this driver id:{} index:{} masterId:{}", id, index, masterId);

            Configuration config = ClusterUtils.loadConfiguration();
            config.setMasterId(masterId);

            String supervisorPort = ClusterUtils.getEnvValue(System.getenv(), ENV_SUPERVISOR_PORT);
            config.put(SUPERVISOR_RPC_PORT, supervisorPort);
            String agentPort = ClusterUtils.getEnvValue(System.getenv(), ENV_AGENT_PORT);
            config.put(AGENT_HTTP_PORT, agentPort);
            LOGGER.info("Supervisor rpc port: {} agentPort: {}", supervisorPort, agentPort);

            new RunnerRuntimeHook(DriverRunner.class.getSimpleName(),
                Integer.parseInt(supervisorPort)).start();

            DriverRunner driverRunner = new DriverRunner(Integer.parseInt(id),
                Integer.parseInt(index), config);
            driverRunner.run();
            LOGGER.info("Completed driver init in {} ms", System.currentTimeMillis() - startTime);
            driverRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            System.exit(EXIT_CODE);
        }
    }

}
