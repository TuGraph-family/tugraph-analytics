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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.AutoRestartPolicy;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterFailoverStrategy extends AbstractKubernetesFailoverStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClusterFailoverStrategy.class);

    private AtomicBoolean doKilling;

    @Override
    public void init(ClusterContext context) {
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.FALSE.getValue());
        doKilling = new AtomicBoolean(context.isRecover());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        // If master does not restart, means master should kill itself.
        // Or else if master restarts, means master should restart all containers and drivers.
        boolean isMasterRestarts = componentId == DEFAULT_MASTER_ID;
        if (isMasterRestarts) {
            final long startTime = System.currentTimeMillis();
            clusterManager.restartAllDrivers();
            clusterManager.restartAllContainers();
            doKilling.set(false);
            LOGGER.info("Completed failover in {} ms", System.currentTimeMillis() - startTime);
        } else if (doKilling.compareAndSet(false, true)) {
            String reason = cause == null ? null : cause.getMessage();
            LOGGER.info("Start master failover triggered by component #{} reason: {}.",
                componentId, reason);
            System.exit(EXIT_CODE);
        }
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.cluster_fo;
    }
}
