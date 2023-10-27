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

package com.antgroup.geaflow.cluster.rpc;

import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.IHAService;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.rpc.proto.Container;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class RpcEndpointRefTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcEndpointRefTest.class);

    private String testHost = "1.2.3.4";
    private int testPort = 1234;

    @Test
    public void testMasterAsyncHeartbeat() throws InterruptedException {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS, "1000");
        configuration.put(ExecutionConfigKeys.HA_SERVICE_TYPE, "memory");

        RpcClient.init(configuration);
        String componentId = "master_id";
        IHAService ihaService = HAServiceFactory.getService(configuration);
        ResourceData resourceData = new ResourceData();
        resourceData.setHost(testHost);
        resourceData.setRpcPort(testPort);
        ihaService.register(componentId, resourceData);
        Assert.assertNotNull(ihaService.resolveResource(componentId));
        // Add to cache.
        RpcClient.getInstance().getResourceData(componentId);

        MasterEndpointRef masterEndpointRef =
            new MasterEndpointRef(testHost, testPort, configuration);

        AtomicReference<String> msg = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ListenableFuture<Empty> future = masterEndpointRef.sendHeartBeat(new Heartbeat(1));

        RpcClient.getInstance().handleFutureCallback(future, new RpcEndpointRef.RpcCallback<Empty>() {

            @Override
            public void onSuccess(Empty event) {
                LOGGER.info("on success");
                msg.set("success");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("on failure");
                msg.set("failure " + t.getMessage());
                countDownLatch.countDown();
            }
        }, componentId);

        countDownLatch.await(4, TimeUnit.SECONDS);
        SleepUtils.sleepMilliSecond(100);
        Assert.assertEquals("failure UNAVAILABLE: io exception", msg.get());
        Assert.assertNull(ihaService.resolveResource(componentId));
    }

    @Test(expected = StatusRuntimeException.class)
    public void testDriverStreamPipeline() {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS, "1000");

        DriverEndpointRef driverEndpointRef =
            new DriverEndpointRef(testHost, testPort, configuration);
        driverEndpointRef.executePipeline(null);
    }

    @Test
    public void testContainerProcess() throws InterruptedException {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS, "1000");
        configuration.put(ExecutionConfigKeys.HA_SERVICE_TYPE, "memory");
        ContainerEndpointRef containerEndpointRef =
            new ContainerEndpointRef(testHost, testPort, configuration);

        RpcClient.init(configuration);
        String componentId = "container_id";
        IHAService ihaService = HAServiceFactory.getService(configuration);
        ResourceData resourceData = new ResourceData();
        resourceData.setHost(testHost);
        resourceData.setRpcPort(testPort);
        ihaService.register(componentId, resourceData);
        Assert.assertNotNull(ihaService.resolveResource(componentId));
        // Add to cache.
        RpcClient.getInstance().getResourceData(componentId);

        AtomicReference<String> msg = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ListenableFuture<Container.Response> future = containerEndpointRef.process(null);
        RpcClient.getInstance().handleFutureCallback(future, new RpcEndpointRef.RpcCallback<Container.Response>() {

            @Override
            public void onSuccess(Container.Response event) {
                LOGGER.info("on success");
                msg.set("success");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("on failure");
                msg.set("failure " + t.getMessage());
                countDownLatch.countDown();
            }
        }, componentId);

        countDownLatch.await(4, TimeUnit.SECONDS);
        Assert.assertEquals("failure UNAVAILABLE: io exception", msg.get());
        SleepUtils.sleepMilliSecond(100);
        Assert.assertNull(ihaService.resolveResource(componentId));
    }

    @Test(expected = ExecutionException.class)
    public void testContainerProcessFuture() throws InterruptedException, ExecutionException {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS, "1000");
        ContainerEndpointRef containerEndpointRef =
            new ContainerEndpointRef("1.2.3.4", 1234, configuration);

        Future<?> future = containerEndpointRef.process(null);
        future.get();
    }

    @Test
    public void testFuture() throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        SettableFuture future = SettableFuture.create();

        AtomicInteger result = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        handleFutureCallback(future, new RpcEndpointRef.RpcCallback<Object>() {
            @Override
            public void onSuccess(Object value) {
                LOGGER.info("on success");
                result.set(1);
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("on failure");
                result.set(2);
                countDownLatch.countDown();
            }
        }, executorService);

        future.set("test");

        countDownLatch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(1, result.get());
        Assert.assertEquals("test", future.get());

        System.out.println("final result" + future.get());
    }

    public <T> void handleFutureCallback(ListenableFuture<T> future,
                                         RpcEndpointRef.RpcCallback<T> listener,
                                         ExecutorService executorService) {
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable T result) {
                listener.onSuccess(result);
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("rpc call failed " + t);
                listener.onFailure(t);
            }
        }, executorService);
    }
}