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

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.SleepUtils;
import java.net.URI;
import java.util.Collections;
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
        AbstractClusterManager clusterManager = Mockito.mock(AbstractClusterManager.class);
        HeartbeatManager heartbeatManager = new HeartbeatManager(configuration, clusterManager);
        HttpServer httpServer = new HttpServer(configuration, clusterManager, heartbeatManager);
        new Thread(new Runnable() {
            @Override
            public void run() {
                httpServer.start();
                SleepUtils.sleepSecond(3);
            }
        }).start();

        Mockito.when(clusterManager.getContainerInfos()).thenReturn(Collections.EMPTY_MAP);

        SleepUtils.sleepSecond(1);
        doGet("http://localhost:8090/", "rest/cluster");
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
