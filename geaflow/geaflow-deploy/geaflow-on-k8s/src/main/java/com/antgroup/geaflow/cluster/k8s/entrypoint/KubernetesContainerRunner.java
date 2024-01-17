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

package com.antgroup.geaflow.cluster.k8s.entrypoint;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;

import com.antgroup.geaflow.cluster.container.Container;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesContainerParam;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.common.config.Configuration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Flink's org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner.
 */
public class KubernetesContainerRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesContainerRunner.class);

    /** The process environment variables. */
    private static final Map<String, String> ENV = System.getenv();

    private final ContainerContext containerContext;
    private Container container;

    public KubernetesContainerRunner(ContainerContext containerContext) {
        this.containerContext = containerContext;
    }

    public void run() {
        Configuration config = containerContext.getConfig();
        KubernetesContainerParam workerParam = new KubernetesContainerParam(config);
        container = new Container(workerParam.getRpcPort());
        containerContext.load();
        container.init(containerContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        container.waitTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            final long startTime = System.currentTimeMillis();
            String id = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_CONTAINER_ID);
            String masterId = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_MASTER_ID);
            boolean isRecover = Boolean.parseBoolean(ClusterUtils.getEnvValue(ENV,
                K8SConstants.ENV_IS_RECOVER));
            LOGGER.info("ResourceID assigned for this container:{} masterId:{}, isRecover:{}", id,
                masterId, isRecover);

            Configuration config = KubernetesUtils.loadConfiguration();
            config.setMasterId(masterId);
            ContainerContext context = new ContainerContext(Integer.parseInt(id), config, isRecover);
            KubernetesContainerRunner kubernetesContainerRunner = new KubernetesContainerRunner(
                context);
            kubernetesContainerRunner.run();
            LOGGER.info("Completed container init in {}ms", System.currentTimeMillis() - startTime);
            kubernetesContainerRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            System.exit(EXIT_CODE);
        }
    }

}
