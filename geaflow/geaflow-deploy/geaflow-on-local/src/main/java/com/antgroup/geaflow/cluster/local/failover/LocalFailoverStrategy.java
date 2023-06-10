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

package com.antgroup.geaflow.cluster.local.failover;

import static com.antgroup.geaflow.cluster.failover.FailoverStrategyType.disable_fo;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFailoverStrategy implements IFailoverStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFailoverStrategy.class);

    @Override
    public void init(ClusterContext context) {

    }

    @Override
    public void doFailover(int componentId) {
        LOGGER.info("component {} do failover", componentId);
    }

    @Override
    public FailoverStrategyType getType() {
        return disable_fo;
    }

    @Override
    public EnvType getEnv() {
        return EnvType.LOCAL;
    }
}
