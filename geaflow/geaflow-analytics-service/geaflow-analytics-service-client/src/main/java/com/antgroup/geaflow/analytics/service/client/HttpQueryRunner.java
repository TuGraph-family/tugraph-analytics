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

import static com.antgroup.geaflow.analytics.service.client.AnalyticsManagerOptions.createClientSession;
import static com.antgroup.geaflow.analytics.service.client.jdbc.AnalyticsResultSet.resultsException;

import com.antgroup.geaflow.analytics.service.client.jdbc.HttpQueryChannel;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.QueryStatusInfo;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.pipeline.service.ServiceType;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class HttpQueryRunner implements IQueryRunner {

    private AtomicReference<AnalyticsManagerSession> session;

    private HttpQueryExecutor queryExecutor;

    @Override
    public void init(ClientHandlerContext handlerContext) {
        queryExecutor = new HttpQueryExecutor(handlerContext);
        queryExecutor.initManagedChannel();
        session = new AtomicReference<>();
    }

    @Override
    public QueryResults executeQuery(String queryScript, HostAndPort address) {
        initClientSession(address);
        HttpQueryChannel httpQueryChannel = queryExecutor.getHttpQueryChannel(address);
        IAnalyticsManager statementClient = httpQueryChannel.executeQuery(session.get(), queryScript);
        if (statementClient.isFinished()) {
            QueryStatusInfo finalStatusInfo = statementClient.getFinalStatusInfo();
            if (finalStatusInfo.getError() != null) {
                throw resultsException(finalStatusInfo);
            }
        }
        return statementClient.getCurrentQueryResult();
    }

    private void initClientSession(HostAndPort address) {
        AnalyticsManagerSession clientSession = createClientSession(address.getHost(), address.getPort());
        session.set(clientSession);
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_http;
    }

    @Override
    public QueryResults cancelQuery(long queryId) {
        return null;
    }

    @Override
    public void close() throws IOException {
        if (queryExecutor != null) {
            queryExecutor.shutdown();
        }
    }
}
