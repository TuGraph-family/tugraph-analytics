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
import com.antgroup.geaflow.cluster.rpc.impl.DefaultRpcCallbackImpl;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.ConfigurableServerOption;
import com.antgroup.geaflow.common.utils.PortUtil;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 100;
        List<IEvent> request = new ArrayList<>();
        List<Future<IEvent>> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            IEvent event = new TestEvent(i);
            request.add(event);
            events.add(new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>())));
        }
        validateResult(events, eventCount, 5000);
        LOGGER.info("send event finish");
    }

    @Test
    public void testShutdownChannel() throws Exception {
        Server server = new Server();
        server.startServer();
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 1000;
        List<Integer> eventIds = new ArrayList<>();
        List<Integer> processedIds = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            event.processTimeMs = 1;
            Future<IEvent> future = new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>()));
            processedIds.add(((TestEvent) (future.get(1000, TimeUnit.MILLISECONDS))).id);
            eventIds.add(i);
            // Do shutdown.
            if (i % 100 == 0) {
                LOGGER.info("shutdown channel");
                server.stopServer();
                LOGGER.info("shutdown channel finish");
                SleepUtils.sleepMilliSecond(10);
            }
        }

        Assert.assertEquals(processedIds, eventIds);
        LOGGER.info("send event finish");
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testServerError() throws Exception {

        Server server = new Server();
        server.startServer();
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 100;
        List<Future<IEvent>> results = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            if (i == 50) {
                event.isException = true;
            }
            results.add(new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>())));
        }
        LOGGER.info("send event finish");
        validateResult(results, eventCount, 5000);
    }

    public void validateResult(List<Future<IEvent>> results, int count, int waitTimeMs)
        throws Exception {
        List<Integer> eventIds = new ArrayList<>();
        List<Integer> processedIds = new ArrayList<>();
        LOGGER.info("validate result");
        for (int i = 0; i < count; i++) {
            eventIds.add(i);
            processedIds.add(
                ((TestEvent) (results.get(i).get(waitTimeMs, TimeUnit.MILLISECONDS))).id);
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
            this.rpcService = new RpcServiceImpl(PortUtil.getPort(rpcPort),
                ConfigurableServerOption.build(configuration));
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
    public class MockContainerEndpoint implements IContainerEndpoint {

        public MockContainerEndpoint() {
        }

        @Override
        public Response process(Request request) {
            try {
                IEvent event = RpcMessageEncoder.decode(request.getPayload());
                if (((TestEvent) event).processTimeMs > 0) {
                    SleepUtils.sleepMilliSecond(((TestEvent) event).processTimeMs);
                }
                if (((TestEvent) event).isException) {
                    LOGGER.info("on error: mock exception");
                    throw new GeaflowRuntimeException("occur mock exception");
                } else {
                    com.antgroup.geaflow.rpc.proto.Container.Response.Builder builder =
                        com.antgroup.geaflow.rpc.proto.Container.Response.newBuilder();
                    builder.setPayload(request.getPayload());
                    return builder.build();
                }
            } catch (Throwable t) {
                LOGGER.error("process request failed: {}", t.getMessage(), t);
                throw new GeaflowRuntimeException("process request failed", t);
            }
        }

        @Override
        public Empty close(Empty request) {
            try {
                LOGGER.info("close");
                return Empty.newBuilder().build();
            } catch (Throwable t) {
                LOGGER.error("close failed: {}", t.getMessage(), t);
                throw new GeaflowRuntimeException(String.format("close failed: %s", t.getMessage()), t);
            }
        }
    }

}
