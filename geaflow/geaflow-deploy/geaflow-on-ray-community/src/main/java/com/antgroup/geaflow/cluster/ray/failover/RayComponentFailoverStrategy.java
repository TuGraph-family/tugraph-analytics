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

package com.antgroup.geaflow.cluster.ray.failover;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.ray.config.RayConfig;
import com.antgroup.geaflow.cluster.runner.failover.ComponentFailoverStrategy;
import com.antgroup.geaflow.env.IEnvironment.EnvType;

public class RayComponentFailoverStrategy extends ComponentFailoverStrategy {

    public RayComponentFailoverStrategy() {
        super(EnvType.RAY_COMMUNITY);
    }

    @Override
    public void init(ClusterContext context) {
        super.init(context);
        System.setProperty(RayConfig.RAY_TASK_RETURN_TASK_EXCEPTION, Boolean.FALSE.toString());
    }

}
