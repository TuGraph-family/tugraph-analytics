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

import com.antgroup.geaflow.cluster.client.utils.PipelineUtil;
import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.RequireResponse;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceInfo;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.resourcemanager.allocator.IAllocator;
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
import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractScheduledWorkerManager implements IScheduledWorkerManager<IExecutionCycle, ExecutionNodeCycle> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScheduledWorkerManager.class);
    private static final String SEPARATOR = "#";
    protected static final String DEFAULT_RESOURCE_ID = "default_";

    protected static final int RETRY_REQUEST_RESOURCE_INTERVAL = 5;
    protected static final int REPORT_RETRY_TIMES = 50;
    protected String masterId;
    protected Map<Long, ResourceInfo> workers;
    protected boolean isAsync;
    protected transient List<ResourceInfo> available;
    protected transient Map<Long, List<WorkerInfo>> assigned;
    protected transient Map<Long, Boolean> isAssigned;
    private static Map<Class, AbstractScheduledWorkerManager> classToInstance = new HashMap<>();

    public AbstractScheduledWorkerManager(Configuration config) {
        this.masterId = config.getMasterId();
        this.isAsync = PipelineUtil.isAsync(config);
    }

    public static AbstractScheduledWorkerManager getInstance(Configuration config, Class<? extends AbstractScheduledWorkerManager> clazz) {
        if (classToInstance.get(clazz) == null) {
            synchronized (AbstractScheduledWorkerManager.class) {
                if (classToInstance.get(clazz) == null) {
                    try {
                        Constructor<? extends AbstractScheduledWorkerManager> constructor = clazz.getDeclaredConstructor(
                            Configuration.class);
                        constructor.setAccessible(true);
                        classToInstance.put(clazz, constructor.newInstance(config));
                    } catch (Throwable e) {
                        throw new GeaflowRuntimeException(String.format("create worker manager %s fail, errorMsg: %s",
                            clazz, e.getMessage()));
                    }
                }
            }
        }
        return classToInstance.get(clazz);
    }

    @Override
    public synchronized void init(IExecutionCycle graph) {

        String resourceId = genResourceId(graph.getDriverIndex(), graph.getSchedulerId());
        LOGGER.info("resource id {}, driver id {}, available {}, isAsync {}",
            resourceId, graph.getDriverId(), available, isAsync);
        if (available == null) {
            available = new ArrayList<>();
            assigned = new ConcurrentHashMap<>();
            isAssigned = new ConcurrentHashMap<>();
        }
        if (workers == null) {
            workers = new ConcurrentHashMap<>();
        }

        if (workers.get(graph.getSchedulerId()) != null) {
            LOGGER.info("reuse workers {}", workers.get(graph.getSchedulerId()));
        } else if (available.size() == 0) {
            List<WorkerInfo> workerInfos = requestWorker(graph, resourceId);
            workers.put(graph.getSchedulerId(), new ResourceInfo(resourceId, workerInfos));
            LOGGER.info("schedulerId: {}, request workers {} from resourceManager",
                graph.getSchedulerId(), workerInfos);
        } else {
            ResourceInfo resourceInfo = available.remove(0);
            workers.put(graph.getSchedulerId(), resourceInfo);
            LOGGER.info("reuse workers {} from available", resourceInfo);
        }
    }

    @Override
    public void clean(CleanWorkerFunction function, IExecutionCycle cycle) {
        function.clean(assigned.get(cycle.getSchedulerId()));
        isAssigned.put(cycle.getSchedulerId(), true);
    }

    @Override
    public synchronized void close(IExecutionCycle cycle) {
        ResourceInfo resourceInfo = this.workers.remove(cycle.getSchedulerId());
        this.available.add(resourceInfo);
        isAssigned.put(cycle.getSchedulerId(), false);
    }

    protected int getExecutionGroupParallelism(ExecutionVertexGroup vertexGroup) {
        return vertexGroup.getVertexMap().values().stream()
            .map(e -> e.getParallelism()).reduce((x, y) -> x + y).get();
    }

    protected List<WorkerInfo> requestWorker(IExecutionCycle graph, String resourceId) {
        int requestResourceNum;
        if (graph.getType() == ExecutionCycleType.GRAPH) {
            requestResourceNum = ((ExecutionGraphCycle) graph).getCycleMap().values().stream()
                .map(e -> ((ExecutionNodeCycle) e).getVertexGroup())
                .map(e -> getExecutionGroupParallelism(e)).max(Integer::compareTo).get();
        } else {
            ExecutionNodeCycle group = (ExecutionNodeCycle) graph;
            requestResourceNum = getExecutionGroupParallelism(group.getVertexGroup());
        }
        IAllocator.AllocateStrategy allocateStrategy = isAsync
            ? IAllocator.AllocateStrategy.PROCESS_FAIR : IAllocator.AllocateStrategy.ROUND_ROBIN;
        RequireResponse response = RpcClient.getInstance().requireResource(masterId,
            RequireResourceRequest.build(resourceId, requestResourceNum,
                allocateStrategy));
        int retryTimes = 1;
        while (!response.isSuccess() || response.getWorkers().isEmpty()) {
            try {
                response = RpcClient.getInstance().requireResource(masterId,
                    RequireResourceRequest.build(resourceId,
                        requestResourceNum,
                        allocateStrategy));
                if (retryTimes % REPORT_RETRY_TIMES == 0) {
                    String msg = String.format("request %s worker with allocateStrategy %s failed after %s times: %s",
                        requestResourceNum, allocateStrategy, retryTimes, response.getMsg());
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
        initWorkers(graph.getSchedulerId(), workers, ScheduledWorkerManagerFactory.getWorkerManagerHALevel(graph));
        return workers;
    }

    protected void initWorkers(long schedulerId, List<WorkerInfo> workers, HighAvailableLevel highAvailableLevel) {
        if (this.workers.get(schedulerId) != null) {
            LOGGER.info("recovered workers {} already init, ignore init again", workers.size());
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

    public String genResourceId(int driverIndex, long schedulerId) {
        return DEFAULT_RESOURCE_ID + driverIndex + SEPARATOR + schedulerId;
    }

    @VisibleForTesting
    public static synchronized void closeInstance() {
        for (Entry<Class, AbstractScheduledWorkerManager> entry : classToInstance.entrySet()) {
            AbstractScheduledWorkerManager workerManager = entry.getValue();
            if (workerManager != null) {
                // Release workers to resourceManager.
                if (workerManager.workers != null) {
                    for (Entry<Long, ResourceInfo> workerEntry : workerManager.workers.entrySet()) {
                        RpcClient.getInstance().releaseResource(workerManager.masterId,
                            ReleaseResourceRequest.build(workerEntry.getValue().getResourceId(),
                                workerEntry.getValue().getWorkers()));
                    }
                }
                if (workerManager.available != null) {
                    for (ResourceInfo resourceInfo : workerManager.available) {
                        RpcClient.getInstance().releaseResource(workerManager.masterId,
                            ReleaseResourceRequest.build(resourceInfo.getResourceId(),
                                resourceInfo.getWorkers()));
                    }
                }
            }
        }
        classToInstance.clear();
    }
}
