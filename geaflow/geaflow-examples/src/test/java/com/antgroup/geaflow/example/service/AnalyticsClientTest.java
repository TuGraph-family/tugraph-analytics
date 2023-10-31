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

import static com.antgroup.geaflow.analytics.service.client.jdbc.AnalyticsDriver.DRIVER_URL_START;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.antgroup.geaflow.analytics.service.client.AnalyticsClient;
import com.antgroup.geaflow.analytics.service.client.jdbc.AnalyticsDriver;
import com.antgroup.geaflow.analytics.service.client.jdbc.AnalyticsResultSet;
import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsServiceConfigKeys;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsClientTest {

    private static final String QUERY = "hello world";

    private static final String RESPONSE = "response";

    @BeforeMethod
    public void setUp() {
        ClusterMetaStore.close();
    }

    @Test
    public void testWordLengthWithAnalyticsClientByRpc() {
        int testPort = 8091;
        String testHost = "localhost";
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(testPort));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_rpc");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        SleepUtils.sleepSecond(3);

        AnalyticsClient analyticsClient = AnalyticsClient.builder()
            .withHost(testHost)
            .withPort(testPort)
            .withRetryNum(3)
            .build();
        QueryResults queryResults = analyticsClient.executeQuery(QUERY);
        Assert.assertNotNull(queryResults);
        List<List<ResponseResult>> list = (List<List<ResponseResult>>) queryResults.getData();
        Assert.assertTrue(list.size() == 1);
        Assert.assertTrue((int) list.get(0).get(0).getResponse().get(0) == 11);
        analyticsClient.shutdown();
        environment.shutdown();
    }

    @Test
    public void testWordLengthWithAnalyticsClientByHttp() {
        int testPort = 8092;
        String testHost = "localhost";
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(testPort));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_http");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        SleepUtils.sleepSecond(3);

        AnalyticsClient analyticsClient = AnalyticsClient.builder()
            .withHost(testHost)
            .withPort(testPort)
            .withConfiguration(configuration)
            .withRetryNum(3).build();
        QueryResults queryResults = analyticsClient.executeQuery(QUERY);
        Assert.assertNotNull(queryResults);
        JSONArray data = (JSONArray) queryResults.getData();
        Object ob = ((JSONArray) data.get(0)).get(0);
        Map<String, Object> jsonResult1 = JSONObject.parseObject(ob.toString(),
            new TypeReference<Map<String, Object>>() {
            }.getType());
        List<Integer> dataList = (List<Integer>) jsonResult1.get(RESPONSE);
        Assert.assertTrue((dataList.get(0) == 11));
        analyticsClient.shutdown();
        environment.shutdown();
    }

    @Test
    public void testWordLengthWithJDBC() {
        int testPort = 8093;
        String testHost = "localhost";
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(testPort));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, QUERY);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_http");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        SleepUtils.sleepSecond(3);

        String url = DRIVER_URL_START + testHost + ":" + testPort;
        Properties properties = new Properties();
        properties.put("user", "analytics_test");
        try {
            Class.forName(AnalyticsDriver.class.getCanonicalName());
            Connection connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            AnalyticsResultSet resultSet = (AnalyticsResultSet) statement.executeQuery(QUERY);
            QueryResults queryResults = resultSet.getQueryResults();
            Assert.assertNotNull(queryResults);

            Assert.assertNotNull(queryResults);
            JSONArray data = (JSONArray) queryResults.getData();
            Object ob = ((JSONArray) data.get(0)).get(0);
            Map<String, Object> jsonResult1 = JSONObject.parseObject(ob.toString(),
                new TypeReference<Map<String, Object>>() {
                }.getType());
            List<Integer> dataList = (List<Integer>) jsonResult1.get(RESPONSE);
            Assert.assertTrue((dataList.get(0) == 11));
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        } finally {
            environment.shutdown();
        }
    }


}
