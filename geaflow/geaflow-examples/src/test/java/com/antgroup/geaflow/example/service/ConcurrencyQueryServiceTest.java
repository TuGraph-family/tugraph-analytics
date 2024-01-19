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

package com.antgroup.geaflow.example.service;

import static com.antgroup.geaflow.file.FileConfigKeys.ROOT;

import com.antgroup.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryRequest;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryResult;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc.AnalyticsServiceBlockingStub;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class ConcurrencyQueryServiceTest extends BaseServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyQueryServiceTest.class);

    private static final Integer THREAD_NUM = 1;

    @Test
    public void testQueryService() throws Exception {
        int port = 8093;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, analyticsQuery);
        configuration.put(ROOT, TEST_GRAPH_PATH);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM, String.valueOf(4));
        // Collection source must be set all window size in order to only execute one batch.
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE, "-1");

        configuration.put(AnalyticsServiceConfigKeys.MAX_REQUEST_PER_SERVER, String.valueOf(THREAD_NUM));
        configuration.put(ExecutionConfigKeys.RPC_IO_THREAD_NUM, "32");
        configuration.put(ExecutionConfigKeys.RPC_WORKER_THREAD_NUM, "32");

        QueryService queryService = new QueryService();
        queryService.submit(environment);

        testQuery(port);
    }

    private void testQuery(int port) throws InterruptedException, ExecutionException {
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

        ManagedChannel channel = buildChannel("localhost", port, 3000);
        ByteString.Output output = ByteString.newOutput();
        SerializerFactory.getKryoSerializer().serialize("request", output);
        QueryRequest request = QueryRequest.newBuilder()
            .setQuery(executeQuery)
            .setQueryConfig(output.toByteString())
            .build();

        List<CompletableFuture<QueryResult>> resultFutureList = new LinkedList<>();
        for (int i = 0; i < THREAD_NUM; i++) {
            AnalyticsServiceBlockingStub stub = AnalyticsServiceGrpc.newBlockingStub(channel);
            LOGGER.info("stub {}: {}", i, stub);
            resultFutureList.add(CompletableFuture.supplyAsync(() -> stub.executeQuery(request)));
        }

        LOGGER.info("resultFutureList: {}", resultFutureList);

        for (int i = 0; i < THREAD_NUM; i++) {
            CompletableFuture<QueryResult> future = resultFutureList.get(i);
            QueryResult queryResult = future.get();
            Assert.assertNotNull(queryResult);
            QueryResults result = RpcMessageEncoder.decode(queryResult.getQueryResult());
            Assert.assertNotNull(result);
            Object defaultFormattedResult = result.getFormattedData();
            List<List<Object>> rawData = result.getRawData();
            Assert.assertEquals(1, rawData.size());
            Assert.assertEquals(3, rawData.get(0).size());
            Assert.assertEquals(rawData.get(0).get(0), 1100001L);
            Assert.assertEquals(rawData.get(0).get(1).toString(), "一");
            Assert.assertEquals(rawData.get(0).get(2).toString(), "王");
            Assert.assertEquals(defaultFormattedResult.toString(), "{\"viewResult\":{\"nodes\":[],\"edges\":[]},\"jsonResult\":[{\"firstName\":\"一\",\"lastName\":\"王\",\"id\":\"1100001\"}]}");
        }
    }
}
