/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.cluster.client.callback;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;

import com.antgroup.geaflow.cluster.rpc.ConnectAddress;
import com.antgroup.geaflow.common.config.Configuration;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RestJobOperatorCallbackTest {

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
        configuration.put(JOB_UNIQUE_ID, String.valueOf(0L));
        RestJobOperatorCallback callback = new RestJobOperatorCallback(configuration, baseUrl);
        Map<String, ConnectAddress> addressList = new HashMap<>();
        addressList.put("1", new ConnectAddress());
        callback.onFinish();

        // confirm that your app made the HTTP requests you were expecting.
        RecordedRequest request1 = server.takeRequest();
        Assert.assertEquals("/api/tasks/0/operations", request1.getPath());
        HttpRequest result1 = new Gson()
                .fromJson(request1.getBody().readString(StandardCharsets.UTF_8), HttpRequest.class);
        Assert.assertTrue(result1.isSuccess());
    }

}
