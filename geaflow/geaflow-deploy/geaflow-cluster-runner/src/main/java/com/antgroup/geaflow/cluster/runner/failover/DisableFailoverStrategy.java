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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisableFailoverStrategy extends AbstractFailoverStrategy {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(DisableFailoverStrategy.class);


    public DisableFailoverStrategy(EnvType envType) {
        super(envType);
    }

    @Override
    public void init(ClusterContext context) {
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.FALSE.getValue());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        LOGGER.info("Failover is disabled, do nothing. Triggered by component #{}: {}.",
            componentId, cause == null ? null : cause.getMessage());
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.disable_fo;
    }
}
