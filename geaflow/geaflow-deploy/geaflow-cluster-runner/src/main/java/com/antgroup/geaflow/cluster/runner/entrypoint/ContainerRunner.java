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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.ENV_AGENT_PORT;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.ENV_SUPERVISOR_PORT;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.IS_RECOVER;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.MASTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.AGENT_HTTP_PORT;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import com.antgroup.geaflow.cluster.common.AbstractContainer;
import com.antgroup.geaflow.cluster.constants.ClusterConstants;
import com.antgroup.geaflow.cluster.container.Container;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.container.IContainer;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.cluster.runner.util.RunnerRuntimeHook;
import com.antgroup.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerRunner.class);
    private final ContainerContext containerContext;
    private IContainer container;

    public ContainerRunner(ContainerContext containerContext) {
        this.containerContext = containerContext;
    }

    public void run() {
        container = new Container();
        containerContext.load();
        container.init(containerContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        ((AbstractContainer) container).waitTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            final long startTime = System.currentTimeMillis();

            String id = ClusterUtils.getProperty(ClusterConstants.CONTAINER_ID);
            String masterId = ClusterUtils.getProperty(MASTER_ID);
            boolean isRecover = Boolean.parseBoolean(
                ClusterUtils.getProperty(IS_RECOVER));
            LOGGER.info("ResourceID assigned for this container:{} masterId:{}, isRecover:{}", id,
                masterId, isRecover);

            Configuration config = ClusterUtils.loadConfiguration();
            config.setMasterId(masterId);

            String supervisorPort = ClusterUtils.getEnvValue(System.getenv(), ENV_SUPERVISOR_PORT);
            config.put(SUPERVISOR_RPC_PORT, supervisorPort);
            String agentPort = ClusterUtils.getEnvValue(System.getenv(), ENV_AGENT_PORT);
            config.put(AGENT_HTTP_PORT, agentPort);
            LOGGER.info("Supervisor rpc port: {} agentPort: {}", supervisorPort, agentPort);

            new RunnerRuntimeHook(ContainerRunner.class.getSimpleName(),
                Integer.parseInt(supervisorPort)).start();

            ContainerContext context = new ContainerContext(Integer.parseInt(id), config,
                isRecover);
            ContainerRunner containerRunner = new ContainerRunner(context);
            containerRunner.run();
            LOGGER.info("Completed container init in {}ms", System.currentTimeMillis() - startTime);
            containerRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            System.exit(EXIT_CODE);
        }
    }

}
