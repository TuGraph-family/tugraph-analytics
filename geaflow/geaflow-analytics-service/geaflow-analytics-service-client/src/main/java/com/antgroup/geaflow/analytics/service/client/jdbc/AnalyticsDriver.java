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

import com.antgroup.geaflow.analytics.service.client.SocketChannelSocketFactory;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import okhttp3.OkHttpClient;

public class AnalyticsDriver implements Driver, Closeable {

    public static final String DRIVER_URL_START = "jdbc:geaflow://";

    private OkHttpClient httpClient;

    static {
        try {
            DriverManager.registerDriver(new AnalyticsDriver());
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("can not register analytics driver", e);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    @Override
    public Connection connect(String url, Properties properties) {
        if (!acceptsURL(url)) {
            return null;
        }
        AnalyticsDriverUri driverUri = new AnalyticsDriverUri(url, properties);
        httpClient = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .build();
        HttpQueryChannel executor = new HttpQueryChannel(httpClient);
        return new AnalyticsConnection(driverUri, executor);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        throw new UnsupportedOperationException();
    }
}
