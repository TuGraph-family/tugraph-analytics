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

public class ComponentFailoverStrategy extends AbstractFailoverStrategy {

    public ComponentFailoverStrategy(EnvType envType) {
        super(envType);
    }

    @Override
    public void init(ClusterContext context) {
        super.init(context);
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.UNEXPECTED.getValue());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.component_fo;
    }

}
