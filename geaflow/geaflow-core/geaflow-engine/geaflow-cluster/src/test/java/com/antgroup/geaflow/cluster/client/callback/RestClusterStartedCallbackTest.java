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

package com.antgroup.geaflow.cluster.client.callback;

import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import com.antgroup.geaflow.cluster.client.callback.RestClusterStartedCallback.HttpRequest;
import com.antgroup.geaflow.common.config.Configuration;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RestClusterStartedCallbackTest {

    MockWebServer server;
    String baseUrl;

    @BeforeClass
    public void prepare() throws IOException {
        // Create a MockWebServer.
        server = new MockWebServer();
        // Schedule some responses.
        server.enqueue(new MockResponse().setBody("{key:value,success:true}"));
        server.enqueue(new MockResponse().setBody("{success:true}"));
        // Start the server.
        server.start();
        baseUrl = "http://" + server.getHostName() + ":" + server.getPort();
    }

    @AfterClass
    public void tearUp() throws IOException {
        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }

    @Test
    public void test() throws InterruptedException {
        // Ask the server for its URL. You'll need this to make HTTP requests.
        Configuration configuration = new Configuration();
        String url = URI.create(baseUrl).resolve("/v1/cluster").toString();
        RestClusterStartedCallback callback = new RestClusterStartedCallback(configuration, url);
        ClusterMeta clusterMeta = new ClusterMeta("driver1", "master1");
        callback.onSuccess(clusterMeta);

        // confirm that your app made the HTTP requests you were expecting.
        RecordedRequest request1 = server.takeRequest();
        Assert.assertEquals("/v1/cluster", request1.getPath());
        HttpRequest result1 = new Gson()
            .fromJson(request1.getBody().readString(StandardCharsets.UTF_8), HttpRequest.class);
        Assert.assertTrue(result1.isSuccess());

        callback.onFailure(new RuntimeException("error"));
        RecordedRequest request2 = server.takeRequest();
        HttpRequest result2 = new Gson()
            .fromJson(request2.getBody().readString(StandardCharsets.UTF_8), HttpRequest.class);
        Assert.assertFalse(result2.isSuccess());
    }

}
