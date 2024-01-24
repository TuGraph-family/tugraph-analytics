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

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.runner.failover.ComponentFailoverStrategy;
import com.antgroup.geaflow.common.exception.GeaflowHeartbeatException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.stats.model.EventLabel;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesComponentFailoverStrategy extends ComponentFailoverStrategy {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(KubernetesComponentFailoverStrategy.class);

    protected ClusterContext clusterContext;

    public KubernetesComponentFailoverStrategy() {
        super(EnvType.K8S);
    }

    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.clusterContext = clusterContext;
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        if (componentId != DEFAULT_MASTER_ID) {
            if (cause instanceof GeaflowHeartbeatException) {
                String startMessage = String.format("Start component failover for component #%s "
                        + "cause by %s.", componentId, cause.getMessage());
                LOGGER.info(startMessage);
                reportFailoverEvent(ExceptionLevel.ERROR, EventLabel.FAILOVER_START, startMessage);

                long startTime = System.currentTimeMillis();
                if (clusterContext.getDriverIds().containsKey(componentId)) {
                    clusterManager.restartDriver(componentId);
                } else {
                    clusterManager.restartContainer(componentId);
                }

                String finishMessage = String.format("Completed component failover for component "
                    + "#%s in %s ms.", componentId, System.currentTimeMillis() - startTime);
                LOGGER.info(finishMessage);
                reportFailoverEvent(ExceptionLevel.INFO, EventLabel.FAILOVER_FINISH, finishMessage);
            } else {
                String reason = cause == null ? null : cause.getMessage();
                LOGGER.warn("{} throws exception: {}", componentId, reason);
            }
        }
    }

}
