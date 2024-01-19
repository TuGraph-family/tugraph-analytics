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

package com.antgroup.geaflow.runtime.core.scheduler.resource;

import static org.mockito.ArgumentMatchers.any;

import com.antgroup.geaflow.cluster.resourcemanager.ResourceInfo;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Container;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AbstractScheduledWorkerManagerTest {

    @AfterMethod
    public void afterMethod() {
        AbstractScheduledWorkerManager.closeInstance();
    }

    @Test
    public void testInitWorkerSuccess() {
        // mock resource manager rpc
        RpcClient rpcClient = Mockito.mock(RpcClient.class);
        MockedStatic<RpcClient> rpcClientMs = Mockito.mockStatic(RpcClient.class);
        rpcClientMs.when(() -> RpcClient.getInstance()).then(invocation -> rpcClient);

        AtomicInteger count = new AtomicInteger(0);
        Mockito.doAnswer(in -> {
            RpcEndpointRef.RpcCallback<Container.Response> callback = ((RpcEndpointRef.RpcCallback) in.getArgument(2));
            callback.onSuccess(null);
            count.incrementAndGet();
            return null;
        }).when(rpcClient).processContainer(any(), any(), any());

        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            workers.add(new WorkerInfo("", 0, 0, 0, i, "worker-" + i));
        }
        AbstractScheduledWorkerManager workerManager = buildMockWorkerManager();
        workerManager.workers = new ConcurrentHashMap<>();
        workerManager.initWorkers(0L, workers, null);
        Assert.assertEquals(count.get(), workers.size());

        count.set(0);
        workerManager.workers.put(0L, new ResourceInfo(workerManager.genResourceId(0, 0L), workers));
        workerManager.initWorkers(0L, workers, null);
        Assert.assertEquals(count.get(), 0);
        rpcClientMs.close();
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInitWorkerFailed() throws Exception {
        // mock resource manager rpc
        RpcClient rpcClient = Mockito.mock(RpcClient.class);
        MockedStatic<RpcClient> rpcClientMs; rpcClientMs = Mockito.mockStatic(RpcClient.class);
        rpcClientMs.when(() -> RpcClient.getInstance()).then(invocation -> rpcClient);

        Mockito.doAnswer(in -> {
            RpcEndpointRef.RpcCallback<Container.Response> callback = ((RpcEndpointRef.RpcCallback) in.getArgument(2));
            callback.onFailure(new GeaflowRuntimeException("rpc error"));
            return null;
        }).when(rpcClient).processContainer(any(), any(), any());

        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            workers.add(new WorkerInfo("", 0, 0, 0, i, "worker-" + i));
        }
        AbstractScheduledWorkerManager workerManager = buildMockWorkerManager();
        workerManager.workers = new ConcurrentHashMap<>();
        try {
            workerManager.initWorkers(0L, workers, null);
        } finally {
            rpcClientMs.close();
        }
    }

    private AbstractScheduledWorkerManager buildMockWorkerManager() {
        return new AbstractScheduledWorkerManager(new Configuration()) {
            @Override
            public List<WorkerInfo> assign(ExecutionNodeCycle vertex) {
                return null;
            }

            @Override
            public void release(ExecutionNodeCycle vertex) {

            }
        };
    }
}
