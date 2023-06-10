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

import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResponse;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.rpc.proto.Container;
import com.antgroup.geaflow.runtime.core.protocol.ComposeEvent;
import com.antgroup.geaflow.runtime.core.protocol.CreateTaskEvent;
import com.antgroup.geaflow.runtime.core.protocol.CreateWorkerEvent;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractScheduledWorkerManager implements IScheduledWorkerManager<IExecutionCycle, ExecutionNodeCycle> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScheduledWorkerManager.class);
    protected static final String DEFAULT_RESOURCE_ID = "default";

    protected static final int RETRY_REQUEST_RESOURCE_INTERVAL = 5;
    protected static final int REPORT_RETRY_TIMES = 50;
    protected String masterId;
    protected List<WorkerInfo> workers;
    protected transient List<WorkerInfo> available;
    protected transient Set<WorkerInfo> assigned;
    protected transient boolean isAssigned = false;

    public AbstractScheduledWorkerManager(Configuration config) {
        this.masterId = config.getMasterId();
    }

    @Override
    public void init(IExecutionCycle graph) {
        if (available == null) {
            workers = requestWorker(graph);
            available = new ArrayList<>(workers);
            LOGGER.info("request workers {}", workers.size());
        } else {
            LOGGER.info("reuse workers {}", workers.size());
        }
        assigned = new HashSet<>();
    }

    @Override
    public void clean(CleanWorkerFunction function) {
        function.clean(assigned);
        isAssigned = true;
    }

    @Override
    public void close() {
        RpcClient.getInstance().releaseResource(masterId, ReleaseResourceRequest.build(DEFAULT_RESOURCE_ID, workers));
    }

    protected int getExecutionGroupParallelism(ExecutionVertexGroup vertexGroup) {
        return vertexGroup.getVertexMap().values().stream()
            .map(e -> e.getParallelism()).reduce((x, y) -> x + y).get();
    }

    protected List<WorkerInfo> requestWorker(IExecutionCycle graph) {
        int requestResourceNum;
        if (graph.getType() == ExecutionCycleType.GRAPH) {
            requestResourceNum = ((ExecutionGraphCycle) graph).getCycleMap().values().stream()
                .map(e -> ((ExecutionNodeCycle) e).getVertexGroup())
                .map(e -> getExecutionGroupParallelism(e)).max(Integer::compareTo).get();
        } else {
            ExecutionNodeCycle group = (ExecutionNodeCycle) graph;
            requestResourceNum = getExecutionGroupParallelism(group.getVertexGroup());
        }
        RequireResponse response = RpcClient.getInstance().requireResource(masterId,
            RequireResourceRequest.build(DEFAULT_RESOURCE_ID, requestResourceNum));
        int retryTimes = 1;
        while (!response.isSuccess() || response.getWorkers().isEmpty()) {
            try {
                response = RpcClient.getInstance().requireResource(masterId,
                    RequireResourceRequest.build(DEFAULT_RESOURCE_ID, requestResourceNum));
                if (retryTimes % REPORT_RETRY_TIMES == 0) {
                    String msg = String.format("request %s worker failed after %s times: %s",
                        requestResourceNum, retryTimes, response.getMsg());
                    LOGGER.warn(msg);
                }
                Thread.sleep(RETRY_REQUEST_RESOURCE_INTERVAL * retryTimes);
                // TODO Report to ExceptionCollector.
                retryTimes++;
            } catch (InterruptedException e) {
                throw new GeaflowRuntimeException(e);
            }
        }

        List<WorkerInfo> workers = response.getWorkers();
        initWorkers(workers, graph.getHighAvailableLevel());
        return workers;
    }

    protected void initWorkers(List<WorkerInfo> workers, HighAvailableLevel highAvailableLevel) {
        if (this.workers != null) {
            LOGGER.info("recovered workers {] already init, ignore init again", workers.size());
            return;
        }
        LOGGER.info("do init workers {}", workers.size());
        CountDownLatch processCountDownLatch = new CountDownLatch(workers.size());
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Throwable> exception = new AtomicReference();
        for (WorkerInfo workerInfo : workers) {
            int workerId = workerInfo.getWorkerIndex();
            CreateTaskEvent createTaskEvent = new CreateTaskEvent(workerId, highAvailableLevel);
            CreateWorkerEvent createWorkerEvent = new CreateWorkerEvent(workerId, highAvailableLevel);
            ComposeEvent composeEvent = new ComposeEvent(workerId, Arrays.asList(createTaskEvent, createWorkerEvent));

            RpcClient.getInstance().processContainer(workerInfo.getContainerName(), composeEvent,
                new RpcEndpointRef.RpcCallback<Container.Response>() {
                    @Override
                    public void onSuccess(Container.Response value) {
                        processCountDownLatch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        processCountDownLatch.countDown();
                        failureCount.incrementAndGet();
                        exception.compareAndSet(null, t);
                    }
                });
        }
        try {
            processCountDownLatch.await();
            LOGGER.info("do init workers finished");
            if (failureCount.get() > 0) {
                throw new GeaflowRuntimeException(String.format("init worker failed. failed count %s",
                    failureCount.get()), exception.get());
            }
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }
}
