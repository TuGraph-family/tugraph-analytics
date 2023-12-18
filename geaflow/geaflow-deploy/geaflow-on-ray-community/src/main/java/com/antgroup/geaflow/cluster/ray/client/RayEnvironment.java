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

package com.antgroup.geaflow.cluster.ray.client;

import static com.antgroup.geaflow.cluster.ray.config.RayConfig.RAY_LOG_DIR;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.LOG_DIR;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_ENABLE;

import com.antgroup.geaflow.cluster.client.AbstractEnvironment;
import com.antgroup.geaflow.cluster.client.IClusterClient;

public class RayEnvironment extends AbstractEnvironment {

    public RayEnvironment() {
        context.getConfig().put(LOG_DIR, RAY_LOG_DIR);
        context.getConfig().put(SUPERVISOR_ENABLE, Boolean.TRUE.toString());
    }

    @Override
    protected IClusterClient getClusterClient() {
        return new RayClusterClient();
    }

    @Override
    public EnvType getEnvType() {
        return EnvType.RAY_COMMUNITY;
    }
}
