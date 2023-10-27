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
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.AutoRestartPolicy;
import com.antgroup.geaflow.common.exception.GeaflowHeartbeatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesComponentFailoverStrategy extends AbstractKubernetesFailoverStrategy {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(KubernetesComponentFailoverStrategy.class);

    protected ClusterContext clusterContext;

    @Override
    public void init(ClusterContext context) {
        this.clusterContext = context;
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.UNEXPECTED.getValue());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        if (componentId != DEFAULT_MASTER_ID) {
            if (cause instanceof GeaflowHeartbeatException) {
                long startTime = System.currentTimeMillis();
                if (clusterContext.getDriverIds().containsKey(componentId)) {
                    clusterManager.restartDriver(componentId);
                } else {
                    clusterManager.restartContainer(componentId);
                }
                LOGGER.info("Completed failover in {} ms", System.currentTimeMillis() - startTime);
            } else {
                String reason = cause == null ? null : cause.getMessage();
                LOGGER.warn("{} throws exception: {}", componentId, reason);
            }
        }
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.component_fo;
    }
}
