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

package com.antgroup.geaflow.cluster.failover;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.env.IEnvironment.EnvType;

public interface IFailoverStrategy {

    /**
     * Init fo strategy by context.
     */
    void init(ClusterContext context);

    /**
     * Trigger failover by input component id.
     */
    void doFailover(int componentId);

    /**
     * Get failover strategy name.
     */
    FailoverStrategyType getType();

    /**
     * Get Env type.
     * @return
     */
    EnvType getEnv();

}
