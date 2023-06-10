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

package com.antgroup.geaflow.cluster.client;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_STARTED_CALLBACK_URL;

import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback;
import com.antgroup.geaflow.cluster.client.callback.RestClusterStartedCallback;
import com.antgroup.geaflow.cluster.client.callback.SimpleClusterStartedCallback;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractClusterClient implements IClusterClient {

    private static final String MASTER_ID = "_MASTER";

    protected String clusterId;
    protected String masterId;
    protected Configuration config;
    protected ClusterStartedCallback callback;

    public void init(IEnvironmentContext environmentContext) {
        EnvironmentContext context = (EnvironmentContext) environmentContext;
        this.config = context.getConfig();

        this.clusterId = config.getString(CLUSTER_ID);
        if (StringUtils.isEmpty(clusterId)) {
            clusterId = UUID.randomUUID().toString();
            config.put(CLUSTER_ID, clusterId);
        }
        this.masterId = MASTER_ID;
        this.config.setMasterId(masterId);
        this.callback = createClusterStartedCallback(config);
    }

    private ClusterStartedCallback createClusterStartedCallback(Configuration configuration) {
        String callbackUrl = configuration.getString(CLUSTER_STARTED_CALLBACK_URL);
        if (StringUtils.isNotBlank(callbackUrl)) {
            return new RestClusterStartedCallback(configuration, callbackUrl);
        }
        return new SimpleClusterStartedCallback();
    }
}
