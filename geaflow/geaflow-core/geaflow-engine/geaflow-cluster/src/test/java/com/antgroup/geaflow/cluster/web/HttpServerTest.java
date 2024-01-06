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

package com.antgroup.geaflow.cluster.web;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HA_SERVICE_TYPE;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.common.ComponentInfo;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.resourcemanager.DefaultResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.ha.service.HAServiceType;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HttpServerTest {

    @Test
    public void test() throws Exception {
        Configuration configuration = new Configuration();
        configuration.put(HA_SERVICE_TYPE, HAServiceType.memory.name());
        AbstractClusterManager clusterManager = Mockito.mock(AbstractClusterManager.class);
        HeartbeatManager heartbeatManager = new HeartbeatManager(configuration, clusterManager);
        IResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        HttpServer httpServer = new HttpServer(configuration, clusterManager, heartbeatManager,
            resourceManager, new ComponentInfo());
        CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                httpServer.start();
                latch.countDown();
            }
        }).start();

        Mockito.when(clusterManager.getContainerInfos()).thenReturn(Collections.EMPTY_MAP);

        latch.await();
        doGet("http://localhost:8090/", "rest/overview");
        doGet("http://localhost:8090/", "rest/containers");
        doGet("http://localhost:8090/", "rest/drivers");
        doGet("http://localhost:8090/", "rest/master/configuration");
        doGet("http://localhost:8090/", "rest/pipelines");
        doGet("http://localhost:8090/", "rest/pipelines/1/cycles");
        httpServer.stop();
    }

    private void doGet(String url, String path) throws Exception {
        URI uri = new URI(url);
        String fullUrl = uri.resolve(path).toString();
        Request request = new Request.Builder().url(fullUrl)
            .get().build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertTrue(response.isSuccessful());
            Assert.assertNotNull(responseBody);
            Assert.assertNotNull(responseBody.string());
        }
    }

}
