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

package com.antgroup.geaflow.cluster.runner.failover;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.stats.model.EventLabel;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This strategy is to restart the whole cluster by the master once an anomaly is detected.
 */
public class ClusterFailoverStrategy extends AbstractFailoverStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterFailoverStrategy.class);

    protected AtomicBoolean doKilling;
    protected HeartbeatManager heartbeatManager;

    public ClusterFailoverStrategy(EnvType envType) {
        super(envType);
    }

    @Override
    public void init(ClusterContext context) {
        super.init(context);
        this.heartbeatManager = context.getHeartbeatManager();
        // Set true if in recovering and reset to false after recovering finished.
        this.doKilling = new AtomicBoolean(context.isRecover());
        // Disable worker process auto-restart because master will do that.
        context.getConfig().put(PROCESS_AUTO_RESTART, Boolean.FALSE.toString());
        LOGGER.info("init with recovering: {}", context.isRecover());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        boolean isMasterRestarts = (componentId == DEFAULT_MASTER_ID);
        if (isMasterRestarts) {
            // Master restart itself when the process is started in recover mode.
            final long startTime = System.currentTimeMillis();
            clusterManager.restartAllDrivers();
            clusterManager.restartAllContainers();
            doKilling.set(false);
            String finishMessage = String.format("Completed failover in %s ms.",
                System.currentTimeMillis() - startTime);
            LOGGER.info(finishMessage);
            reportFailoverEvent(ExceptionLevel.INFO, EventLabel.FAILOVER_FINISH, finishMessage);
        } else if (doKilling.compareAndSet(false, true)) {
            String reason = cause == null ? null : cause.getMessage();
            String startMessage = String.format("Start failover due to %s", reason);
            LOGGER.info(startMessage);
            reportFailoverEvent(ExceptionLevel.INFO, EventLabel.FAILOVER_START, startMessage);
            // Close heartbeat check service.
            heartbeatManager.close();
            // Trigger process restart.
            System.exit(EXIT_CODE);
        }
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.cluster_fo;
    }

}
