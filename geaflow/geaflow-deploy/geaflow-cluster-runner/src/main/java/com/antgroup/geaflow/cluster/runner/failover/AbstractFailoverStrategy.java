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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_ENABLE;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import com.antgroup.geaflow.stats.model.EventLabel;
import com.antgroup.geaflow.stats.model.ExceptionLevel;

public abstract class AbstractFailoverStrategy implements IFailoverStrategy {

    protected EnvType envType;
    protected AbstractClusterManager clusterManager;
    protected boolean enableSupervisor;
    protected ClusterContext context;

    public AbstractFailoverStrategy(EnvType envType) {
        this.envType = envType;
    }

    @Override
    public void init(ClusterContext context) {
        this.context = context;
        this.enableSupervisor = context.getConfig().getBoolean(SUPERVISOR_ENABLE);
    }

    protected void reportFailoverEvent(ExceptionLevel level, EventLabel label, String message) {
        StatsCollectorFactory.init(context.getConfig()).getEventCollector()
            .reportEvent(level, label, message);
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = (AbstractClusterManager) clusterManager;
    }

    @Override
    public EnvType getEnv() {
        return envType;
    }

}
