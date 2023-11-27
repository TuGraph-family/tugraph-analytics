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

package com.antgroup.geaflow.analytics.service.client;

import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsClientConfigKeys;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import java.util.List;

public abstract class AbstractQueryExecutor {

    protected final List<HostAndPort> coordinatorAddresses;
    protected final int retryNum;
    protected final int timeoutMs;
    protected final Configuration config;

    public AbstractQueryExecutor(ClientHandlerContext context) {
        this.coordinatorAddresses = context.getServiceInfo().getCoordinatorAddresses();
        this.config = context.getConfiguration();
        this.timeoutMs = config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS);
        this.retryNum = config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM);
    }

    protected void initManagedChannel() {
        for (HostAndPort coordinatorAddress : this.coordinatorAddresses) {
            initManagedChannel(coordinatorAddress);
        }
    }

    protected abstract void initManagedChannel(HostAndPort address);

    protected abstract void shutdown();
}
