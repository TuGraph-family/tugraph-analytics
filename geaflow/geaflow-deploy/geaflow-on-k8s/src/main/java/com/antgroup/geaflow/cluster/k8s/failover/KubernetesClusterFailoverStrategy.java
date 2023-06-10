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

package com.antgroup.geaflow.cluster.k8s.failover;

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterFailoverStrategy extends AbstractKubernetesFailoverStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClusterFailoverStrategy.class);

    private volatile boolean doKilling;

    @Override
    public void init(ClusterContext context) {
        context.getConfig().put(PROCESS_AUTO_RESTART, "false");
    }

    @Override
    public void doFailover(int componentId) {

        LOGGER.info("Start cluster failover triggered by component {}.", componentId);

        boolean isMasterRestarts = componentId == 0;

        // If master does not restart, means master should kill itself.
        // Or else if master restarts, means master should restart all containers and drivers.
        if (isMasterRestarts) {

            // Do nothing if master is killing all driver and container pods.
            if (doKilling) {
                return;
            }

            doKilling = true;
            LOGGER.info("Kill all container pods.");
            clusterManager.killAllContainers();
            LOGGER.info("Kill all driver and container pods finish.");

            doKilling = false;
            LOGGER.info("Start all container pods.");
            clusterManager.startAllContainersByFailover();
            LOGGER.info("Start all container pods finish.");

            LOGGER.info("Kill all driver pods.");
            clusterManager.killDriver();
        } else {
            clusterManager.killMaster();
        }
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.cluster_fo;
    }
}
