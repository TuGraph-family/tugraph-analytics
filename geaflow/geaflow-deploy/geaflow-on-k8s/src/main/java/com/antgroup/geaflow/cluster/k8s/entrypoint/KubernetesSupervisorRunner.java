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

import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.CONTAINER_START_COMMAND;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.ENV_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.Supervisor;
import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesSupervisorRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesSupervisorRunner.class);
    private static final Map<String, String> ENV = System.getenv();
    private final Supervisor supervisor;

    public KubernetesSupervisorRunner(Configuration configuration, String startCommand,
                                      List<String> commands, boolean autoRestart) {
        this.supervisor = new Supervisor(startCommand, commands, configuration, autoRestart);
        StatsCollectorFactory.init(configuration);
    }

    public void run() {
        supervisor.start();
    }

    private void waitForTermination() {
        LOGGER.info("Waiting for supervisor exit.");
        supervisor.waitForTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            String id = KubernetesUtils.getEnvValue(ENV, K8SConstants.ENV_CONTAINER_ID);
            String autoRestartEnv = KubernetesUtils.getEnvValue(ENV, ENV_AUTO_RESTART);
            LOGGER.info("Start supervisor with ID: {} pid: {} autoStart:{}", id,
                ProcessUtil.getProcessId(), autoRestartEnv);

            Configuration config = KubernetesUtils.loadConfiguration();
            String startCommand = KubernetesUtils.getEnvValue(ENV, CONTAINER_START_COMMAND);
            List<String> serviceCommands = new ArrayList<>();
            boolean autoRestart = !autoRestartEnv.equalsIgnoreCase(Boolean.FALSE.toString());
            KubernetesSupervisorRunner workerRunner = new KubernetesSupervisorRunner(config,
                startCommand, serviceCommands, autoRestart);
            workerRunner.run();
            workerRunner.waitForTermination();
            LOGGER.info("Exit worker process");
        } catch (Throwable e) {
            LOGGER.error("FETAL: process exits", e);
            throw e;
        }
    }

}
