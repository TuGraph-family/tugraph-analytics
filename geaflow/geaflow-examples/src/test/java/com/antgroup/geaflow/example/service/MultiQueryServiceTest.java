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

import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsServiceConfigKeys;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.metaserver.client.MetaServerQueryClient;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryRequest;
import com.antgroup.geaflow.rpc.proto.Analytics.QueryResult;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiQueryServiceTest extends BaseServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiQueryServiceTest.class);

    @Test
    public void testQueryService() throws Exception {
        before();
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.DRIVER_NUM, "2");
        configuration.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE, Boolean.TRUE.toString());
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, analyticsQuery);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM, "2");
        // Collection source must be set all window size in order to only execute one batch.
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE, "-1");
        configuration.putAll(this.configuration.getConfigMap());

        QueryService queryService = new QueryService();
        queryService.submit(environment);
        testQuery();

        environment.shutdown();
        after();
    }

    private void testQuery() {
        SleepUtils.sleepSecond(10);

        MetaServerQueryClient queryClient = new MetaServerQueryClient(configuration);
        List<HostAndPort> serviceInfos = queryClient.queryAllServices(NamespaceType.DEFAULT);
        for (HostAndPort hostAndPort : serviceInfos) {
            LOGGER.info("host and port {}", hostAndPort);
            ManagedChannel channel = buildChannel(hostAndPort.getHost(), hostAndPort.getPort(), 3000);
            AnalyticsServiceGrpc.AnalyticsServiceBlockingStub stub = AnalyticsServiceGrpc.newBlockingStub(channel);

            ByteString.Output output = ByteString.newOutput();
            SerializerFactory.getKryoSerializer().serialize("request", output);
            QueryRequest request = QueryRequest.newBuilder()
                .setQuery(executeQuery)
                .setQueryConfig(output.toByteString())
                .build();
            QueryResult queryResult = stub.executeQuery(request);
            Assert.assertNotNull(queryResult);
            QueryResults result = RpcMessageEncoder.decode(queryResult.getQueryResult());
            List<List<ResponseResult>> list = (List<List<ResponseResult>>) result.getData();
            LOGGER.info("result list {}", list);
            Assert.assertTrue(list.size() == 1);
            Assert.assertTrue(list.get(0).get(0).getResponse().size() == 0);
        }

        queryClient.close();
    }

}
