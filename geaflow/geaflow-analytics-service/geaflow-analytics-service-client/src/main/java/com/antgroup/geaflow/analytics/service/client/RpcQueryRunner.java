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

import com.antgroup.geaflow.analytics.service.query.QueryError;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.StandardError;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.common.serialize.ISerializer;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.LogMsgUtil;
import com.antgroup.geaflow.pipeline.service.ServiceType;
import com.antgroup.geaflow.rpc.proto.Analytics;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryRequest;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryResult;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc.AnalyticsServiceBlockingStub;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcQueryRunner implements IQueryRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcQueryRunner.class);

    private static final ISerializer SERIALIZER = SerializerFactory.getKryoSerializer();

    private static final String REQUEST = "request";

    private RpcQueryExecutor queryExecutor;

    @Override
    public void init(ClientHandlerContext handlerContext) {
        queryExecutor = new RpcQueryExecutor(handlerContext);
        queryExecutor.initManagedChannel();
    }

    @Override
    public QueryResults executeQuery(String queryScript, HostAndPort address) {
        AnalyticsServiceBlockingStub analyticsServiceBlockingStub = queryExecutor.getAnalyticsServiceBlockingStub(address);
        QueryRequest queryRequest = buildRpcRequest(queryScript);
        QueryResult queryResponse;
        try {
            queryResponse = analyticsServiceBlockingStub.executeQuery(queryRequest);
        } catch (StatusRuntimeException e) {
            LOGGER.error("query {} execute failed with status {} and cause {}", queryScript, e.getStatus(), LogMsgUtil.getStackMsg(e));
            return getResultIfException(e, address);
        }
        QueryResults result = RpcMessageEncoder.decode(queryResponse.getQueryResult());
        LOGGER.info("queryId {} status {}", result.getQueryId(), result.isQueryStatus());
        return result;
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_rpc;
    }

    @Override
    public QueryResults cancelQuery(long queryId) {
        throw new GeaflowRuntimeException("not supports operate");
    }

    private QueryResults getResultIfException(StatusRuntimeException e, HostAndPort address) {
        QueryError queryError;
        switch (e.getStatus().getCode()) {
            case UNAVAILABLE:
                LOGGER.warn("coordinator address {} server unavailable", address);
                queryError = StandardError.ANALYTICS_SERVER_UNAVAILABLE.getQueryError();
                return new QueryResults(queryError);
            case RESOURCE_EXHAUSTED:
                LOGGER.warn("query result too big: {}", e.getMessage());
                queryError = StandardError.ANALYTICS_RESULT_TO_LONG.getQueryError();
                return new QueryResults(queryError);
            default:
                LOGGER.warn("re-init channel {} for unexpected exception: {}", address, e.getStatus());
                queryExecutor.initManagedChannel(address);
                queryError = StandardError.ANALYTICS_RPC_ERROR.getQueryError();
                return new QueryResults(queryError);
        }
    }

    private Analytics.QueryRequest buildRpcRequest(String queryScript) {
        ByteString.Output output = ByteString.newOutput();
        SERIALIZER.serialize(REQUEST, output);
        return Analytics.QueryRequest.newBuilder()
            .setQuery(queryScript)
            .setQueryConfig(output.toByteString())
            .build();
    }

    @Override
    public void close() throws IOException {
        if (queryExecutor != null) {
            queryExecutor.shutdown();
        }
    }
}
