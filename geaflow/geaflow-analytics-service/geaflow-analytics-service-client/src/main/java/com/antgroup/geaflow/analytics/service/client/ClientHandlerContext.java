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

import com.antgroup.geaflow.common.config.Configuration;

public class ClientHandlerContext {

    private final Configuration configuration;

    private final AnalyticsServiceInfo serviceInfo;

    private final boolean initChannelPools;

    public Configuration getConfiguration() {
        return configuration;
    }

    public AnalyticsServiceInfo getServiceInfo() {
        return serviceInfo;
    }

    public boolean enableInitChannelPools() {
        return initChannelPools;
    }

    public static ClientHandlerContextBuilder newBuilder() {
        return new ClientHandlerContextBuilder();
    }

    public static class ClientHandlerContextBuilder {

        private Configuration configuration;

        private AnalyticsServiceInfo serviceInfo;

        private boolean initChannelPools;

        public ClientHandlerContextBuilder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public ClientHandlerContextBuilder setAnalyticsServiceInfo(AnalyticsServiceInfo serviceInfo) {
            this.serviceInfo = serviceInfo;
            return this;
        }

        public ClientHandlerContextBuilder enableInitChannelPools(boolean initChannelPools) {
            this.initChannelPools = initChannelPools;
            return this;
        }

        public ClientHandlerContext build() {
            return new ClientHandlerContext(this);
        }
    }


    private ClientHandlerContext(ClientHandlerContextBuilder builder) {
        this.configuration = builder.configuration;
        this.serviceInfo = builder.serviceInfo;
        this.initChannelPools = builder.initChannelPools;
    }
}
