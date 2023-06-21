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

package com.antgroup.geaflow.ha.service;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_TIMEOUT_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static com.antgroup.geaflow.store.redis.RedisConfigKeys.REDIS_HOST;
import static com.antgroup.geaflow.store.redis.RedisConfigKeys.REDIS_PORT;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.github.fppt.jedismock.RedisServer;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RedisHAServiceTest {

    private RedisServer redisServer;

    @BeforeClass
    public void prepare() throws IOException {
        redisServer = RedisServer.newRedisServer().start();
    }

    @AfterClass
    public void tearUp() throws IOException {
        redisServer.stop();
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void test() {
        RedisHAService haService = new RedisHAService();
        Configuration configuration = new Configuration();
        configuration.put(REDIS_HOST, redisServer.getHost());
        configuration.put(REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        configuration.put(JOB_UNIQUE_ID, "123");
        configuration.put(FO_TIMEOUT_MS, "2000");
        haService.open(configuration);

        ResourceData resourceData = new ResourceData();
        resourceData.setHost(ProcessUtil.getHostname());
        resourceData.setRpcPort(6055);
        haService.register("1", resourceData);
        ResourceData result = haService.resolveResource("1");
        Assert.assertNotNull(result);
        haService.close();
    }

    @Test
    public void testMultiThread() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        RedisHAService haService = new RedisHAService();
        Configuration configuration = new Configuration();
        configuration.put(REDIS_HOST, redisServer.getHost());
        configuration.put(REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        configuration.put(JOB_UNIQUE_ID, "2300087");
        configuration.put(FO_TIMEOUT_MS, "2000");
        haService.open(configuration);

        CountDownLatch latch = new CountDownLatch(1);
        String resourceId = "test-multi-thread";
        executorService.execute(() -> {
            while (true) {
                ResourceData resourceData = new ResourceData();
                resourceData.setHost("abc");
                haService.register(resourceId, resourceData);
                latch.countDown();
            }
        });

        latch.await();
        ResourceData data = haService.getResourceData(resourceId);
        Assert.assertNotNull(data);
        executorService.shutdown();
    }

}
