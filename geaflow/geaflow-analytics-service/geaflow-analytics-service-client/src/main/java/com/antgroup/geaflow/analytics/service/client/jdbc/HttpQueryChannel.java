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

package com.antgroup.geaflow.analytics.service.client.jdbc;

import static com.antgroup.geaflow.analytics.service.client.AnalyticsManagerFactory.buildAnalyticsManager;
import static com.antgroup.geaflow.analytics.service.client.utils.OkHttpUtils.setupTimeouts;
import static com.antgroup.geaflow.analytics.service.client.utils.OkHttpUtils.tokenAuth;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.antgroup.geaflow.analytics.service.client.AnalyticsManagerSession;
import com.antgroup.geaflow.analytics.service.client.IAnalyticsManager;
import com.antgroup.geaflow.analytics.service.client.SocketChannelSocketFactory;
import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsClientConfigKeys;
import com.antgroup.geaflow.common.config.Configuration;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

public class HttpQueryChannel implements Closeable {

    private final OkHttpClient httpClient;

    private Configuration config;

    public HttpQueryChannel(OkHttpClient httpClient) {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public HttpQueryChannel(Configuration config, int timeout) {
        this.config = config;
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.socketFactory(new SocketChannelSocketFactory());
        setupTimeouts(builder, timeout, TimeUnit.MILLISECONDS);
        this.httpClient = builder.build();
    }

    public IAnalyticsManager executeQuery(AnalyticsManagerSession session, String query) {
        return buildAnalyticsManager(httpClient, session, query);
    }

    private void setupTokenAuth(OkHttpClient.Builder clientBuilder,
                                AnalyticsManagerSession clientSession) {
        if (this.config.contains(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_ACCESS_TOKEN)) {
            checkArgument(clientSession.getServer().getScheme().equalsIgnoreCase("https"),
                "Authentication using an access token requires HTTPS to be enabled");
            String accessToken = config.getString(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_ACCESS_TOKEN);
            clientBuilder.addInterceptor(tokenAuth(accessToken));
        }
    }

    @Override
    public void close() throws IOException {
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }

}
