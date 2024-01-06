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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsServiceConfigKeys;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.server.http.handler.HttpAnalyticsServiceHandler;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.rpc.proto.Analytics;
import com.antgroup.geaflow.rpc.proto.AnalyticsServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WordLengthServiceTest extends BaseTest {

    private static final String QUERY = "hello world";

    @Test
    public void testWordLengthWithHttpServer() throws Exception {
        int port = 8091;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_http");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        testHttpServiceServer(port);
    }

    @Test
    public void testWordLengthSourceMultiParallelismWithHttpServer() throws Exception {
        int port = 8092;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_http");
        configuration.put(ExampleConfigKeys.SOURCE_PARALLELISM.getKey(), "3");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        testHttpServiceServer(port);
    }

    @Test
    public void testWordLengthWithRpcServer() {
        int port = 8093;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_rpc");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        testRpcServiceServer(port);
    }

    @Test
    public void testWordLengthSourceMultiParallelismWithRpcServer() {
        int port = 8094;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_rpc");
        configuration.put(ExampleConfigKeys.SOURCE_PARALLELISM.getKey(), "3");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        testRpcServiceServer(port);
    }

    private static void testHttpServiceServer(int port) throws Exception {
        SleepUtils.sleepSecond(3);

        String url = String.format("http://localhost:%s/", port);
        String path = "/rest/analytics/query/execute";
        URI uri = new URI(url);
        String fullUrl = uri.resolve(path).toString();
        RequestBody body = new FormBody.Builder().add("query", QUERY).build();
        Request request = new Request.Builder().post(body).url(fullUrl).build();

        OkHttpClient client = new OkHttpClient().newBuilder().readTimeout(30, TimeUnit.SECONDS).build();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertTrue(response.isSuccessful());
            Assert.assertNotNull(responseBody);

            byte[] resultBytes = responseBody.bytes();
            Map<String, QueryResults> jsonResult = JSONObject.parseObject(resultBytes,
                new TypeReference<Map<String, QueryResults>>() {
                }.getType());

            Assert.assertNotNull(resultBytes);
            JSONArray data = (JSONArray) jsonResult.get(HttpAnalyticsServiceHandler.QUERY_RESULT).getData();
            Assert.assertTrue(data != null);
            Object ob = ((JSONArray) data.get(0)).get(0);
            Map<String, Object> jsonResult1 = JSONObject.parseObject(ob.toString(),
                new TypeReference<Map<String, Object>>() {
                }.getType());
            List<Integer> dataList = (List<Integer>) jsonResult1.get("response");
            Assert.assertTrue((dataList.get(0) == 11));
        }
    }

    private static void testRpcServiceServer(int port) {
        SleepUtils.sleepSecond(3);

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build();
        AnalyticsServiceGrpc.AnalyticsServiceBlockingStub stub = AnalyticsServiceGrpc.newBlockingStub(channel);

        ByteString.Output output = ByteString.newOutput();
        SerializerFactory.getKryoSerializer().serialize("request", output);
        Analytics.QueryRequest request = Analytics.QueryRequest.newBuilder()
            .setQuery(QUERY)
            .setQueryConfig(output.toByteString())
            .build();
        Analytics.QueryResult queryResult = stub.executeQuery(request);
        Assert.assertNotNull(queryResult);
        QueryResults result = RpcMessageEncoder.decode(queryResult.getQueryResult());
        List<List<ResponseResult>> list = (List<List<ResponseResult>>) result.getData();
        Assert.assertTrue(list.size() == 1);
        Assert.assertTrue((int) list.get(0).get(0).getResponse().get(0) == 11);
    }

}
