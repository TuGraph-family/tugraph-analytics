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

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.RpcMessageEncoder;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.common.utils.ReflectionUtil;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.rpc.proto.ContainerServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AsyncRpcTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRpcTest.class);

    @Test
    public void testAsyncRpc() throws Exception {

        Server server = new Server();
        server.startServer();
        ExecutorService  executorService = Executors.newFixedThreadPool(1);
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort, executorService);

        int eventCount = 100;
        List<IEvent> request = new ArrayList<>();
        List<Future<IEvent>> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            IEvent event = new TestEvent(i);
            request.add(event);
            events.add(client.process(event));
        }
        validateResult(events, eventCount, 5000);
        LOGGER.info("send event finish");
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testShutdownChannel() throws Exception {

        Server server = new Server();
        server.startServer();
        ExecutorService  executorService = Executors.newFixedThreadPool(1);
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort, executorService);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                SleepUtils.sleepMilliSecond(300);
                LOGGER.info("shutdown channel");
                ManagedChannel channel = (ManagedChannel) ReflectionUtil.getField(client, "channel");
                channel.shutdownNow();
                LOGGER.info("shutdown channel finish ");
            }
        });
        thread.start();

        int eventCount = 1000;
        List<IEvent> request = new ArrayList<>();
        List<Future<IEvent>> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            request.add(event);
            event.processTimeMs = 100;
            SleepUtils.sleepMilliSecond(1);
            if (i % 100 == 0) {
            }
            events.add(client.process(event));
        }
        validateResult(events, eventCount, 5000);
        LOGGER.info("send event finish");
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testServerError() throws Exception {

        Server server = new Server();
        server.startServer();
        ExecutorService  executorService = Executors.newFixedThreadPool(1);
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort, executorService);

        int eventCount = 100;
        List<Future<IEvent>> results = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            if (i == 50) {
                event.isException = true;
            }
            results.add(client.process(event));
        }
        LOGGER.info("send event finish");
        validateResult(results, eventCount, 5000);
    }

    public void validateResult(List<Future<IEvent>> results, int count, int waitTimeMs) throws Exception {
        List<Integer> eventIds = new ArrayList<>();
        List<Integer> processedIds = new ArrayList<>();
        LOGGER.info("validate result");
        for (int i = 0; i < count; i++) {
            eventIds.add(i);
            processedIds.add(((TestEvent) (results.get(i).get(waitTimeMs, TimeUnit.MILLISECONDS))).id);
        }
        Assert.assertEquals(processedIds, eventIds);
        LOGGER.info("validate finish");
    }

    /**
     * Mock event with dummy info.
     */
    public class TestEvent implements IEvent {
        private int id;
        private int processTimeMs;
        private boolean isException;

        public TestEvent(int id) {
            this.id = id;
        }


        @Override
        public EventType getEventType() {
            return null;
        }

        @Override
        public String toString() {
            return "TestEvent{" +
                "id=" + id +
                '}';
        }
    }

    public class Server {

        protected int rpcPort;

        protected Configuration configuration = new Configuration();
        protected RpcServiceImpl rpcService;

        public void startServer() {
            this.rpcService = new RpcServiceImpl(0, configuration);
            this.rpcService.addEndpoint(new MockContainerEndpoint());
            this.rpcPort = rpcService.startService();
        }

        public void stopServer() {
            rpcService.stopService();
        }
    }

    /**
     * Mock endpoint to process events.
     */
    public class MockContainerEndpoint extends ContainerServiceGrpc.ContainerServiceImplBase implements
        RpcEndpoint {

        public MockContainerEndpoint() {
        }

        public void process(com.antgroup.geaflow.rpc.proto.Container.Request request,
                            StreamObserver<com.antgroup.geaflow.rpc.proto.Container.Response> responseObserver) {
            try {
                IEvent event = RpcMessageEncoder.decode(request.getPayload());
                if (((TestEvent) event).processTimeMs > 0) {
                    SleepUtils.sleepMilliSecond(((TestEvent) event).processTimeMs);
                }
                if (((TestEvent) event).isException) {
                    LOGGER.info("on error: mock exception");
                    responseObserver.onError(new GeaflowRuntimeException("occur mock exception"));
                } else {
                    com.antgroup.geaflow.rpc.proto.Container.Response.Builder builder = com.antgroup.geaflow.rpc.proto.Container.Response.newBuilder();
                    builder.setPayload(request.getPayload());
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                }
            } catch (Throwable t) {
                LOGGER.error("process request failed: {}", t.getMessage(), t);
                responseObserver.onError(t);
            }
        }

        public void close(Empty request,
                          StreamObserver<Empty> responseObserver) {
            try {
                LOGGER.info("close");
                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Throwable t) {
                LOGGER.error("close failed: {}", t.getMessage(), t);
                responseObserver.onError(t);
            }
        }
    }

}
