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
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.AutoRestartPolicy;

public class KubernetesDisableFailoverStrategy extends AbstractKubernetesFailoverStrategy {

    @Override
    public void init(ClusterContext context) {
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.FALSE.getValue());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.disable_fo;
    }
}
