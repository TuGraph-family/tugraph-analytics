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

package com.antgroup.geaflow.runtime.core.scheduler;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionTaskType;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.protocol.ComposeEvent;
import com.antgroup.geaflow.runtime.core.protocol.LaunchSourceEvent;
import com.antgroup.geaflow.runtime.core.protocol.RollbackCycleEvent;
import com.antgroup.geaflow.runtime.core.scheduler.context.CheckpointSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PipelineCycleSchedulerTest extends BaseCycleSchedulerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineCycleSchedulerTest.class);

    private static CheckpointSchedulerContext mockPersistContext;
    private Configuration configuration;

    @BeforeMethod
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put(JOB_UNIQUE_ID.getKey(), "scheduler-fo-test" + System.currentTimeMillis());
        config.put(RUN_LOCAL_MODE.getKey(), "true");
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        config.put(CONTAINER_HEAP_SIZE_MB.getKey(), String.valueOf(1024));
        configuration = new Configuration(config);
        ClusterMetaStore.init(0, configuration);
    }

    @AfterMethod
    public void cleanUp() {
        ClusterMetaStore.close();
    }

    @Test(priority = 0)
    public void testSimplePipeline() {
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);
        ShuffleManager.init(configuration);
        ShuffleManager.getInstance().initShuffleMaster();
        StatsCollectorFactory.init(configuration);

        CheckpointSchedulerContext context = (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(configuration), null);
        mockPersistContext = context;
        scheduler.init(context);
        scheduler.execute();
        scheduler.close();


        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(7, events.size());
        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.CREATE_TASK, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(EventType.CREATE_WORKER, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());

        Assert.assertEquals(EventType.COMPOSE, events.get(1).getEventType());
        Assert.assertEquals(EventType.INIT_CYCLE, ((ComposeEvent) events.get(1)).getEventList().get(0).getEventType());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(1)).getEventList().get(1).getEventType());
        Assert.assertEquals(1, ((LaunchSourceEvent) ((ComposeEvent) events.get(1)).getEventList().get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(2).getEventType());
        Assert.assertEquals(2, ((LaunchSourceEvent) events.get(2)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(5).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(5)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(6).getEventType());

    }

    @Test(priority = 1)
    private void testPipelineAfterRestart() {

        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        // mock recover context from previous case.
        CheckpointSchedulerContext context = mockPersistContext;
        context.init(3);
        context.getSchedulerStateMap().put(context.getCurrentIterationId(),
            Arrays.asList(ICycleSchedulerContext.SchedulerState.INIT,
                ICycleSchedulerContext.SchedulerState.ROLLBACK,
                ICycleSchedulerContext.SchedulerState.EXECUTE));

        scheduler.init(context);
        scheduler.execute();
        scheduler.close();


        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.INIT_CYCLE, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(EventType.ROLLBACK, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());
        Assert.assertEquals(2, ((RollbackCycleEvent) ((ComposeEvent) events.get(0)).getEventList().get(1)).getIterationWindowId());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(0)).getEventList().get(2).getEventType());
        Assert.assertEquals(3, ((LaunchSourceEvent) ((ComposeEvent) events.get(0)).getEventList().get(2)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(1).getEventType());
        Assert.assertEquals(4, ((LaunchSourceEvent) events.get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(2).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(2)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(3).getEventType());
    }

    @Test(priority = 1)
    private void testPipelineAfterRecover() {

        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        // mock recover context from previous case.
        CheckpointSchedulerContext context = mockPersistContext;
        context.init(4);
        context.getSchedulerStateMap().put(context.getCurrentIterationId(),
            Arrays.asList(ICycleSchedulerContext.SchedulerState.ROLLBACK,
                ICycleSchedulerContext.SchedulerState.EXECUTE));

        scheduler.init(context);
        scheduler.execute();
        scheduler.close();


        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(3, events.size());
        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.ROLLBACK, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(3, ((RollbackCycleEvent) ((ComposeEvent) events.get(0)).getEventList().get(0)).getIterationWindowId());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());
        Assert.assertEquals(4, ((LaunchSourceEvent) ((ComposeEvent) events.get(0)).getEventList().get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(1).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(2).getEventType());
    }

    private ExecutionNodeCycle buildMockCycle(Configuration configuration) {

        long finishIterationId = 5;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setIterative(false);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(1);
        vertexGroup.getVertexMap().put(0, vertex);
        vertexGroup.putVertexId2InEdgeIds(0, new ArrayList<>());
        vertexGroup.putVertexId2OutEdgeIds(0, new ArrayList<>());

        List<ExecutionTask> headTasks = new ArrayList<>();
        List<ExecutionTask> tailTasks = new ArrayList<>();
        for (int i = 0; i < vertex.getParallelism(); i++) {
            ExecutionTask task = new ExecutionTask(i, i, vertex.getParallelism(), vertex.getParallelism(), vertex.getParallelism(), vertex.getVertexId());
            task.setExecutionTaskType(ExecutionTaskType.head);
            tailTasks.add(task);
            headTasks.add(task);
        }

        ExecutionNodeCycle cycle = new ExecutionNodeCycle(0, "test", vertexGroup, configuration, "driver_id");
        cycle.setCycleHeads(headTasks);
        cycle.setCycleTails(tailTasks);
        cycle.setTasks(headTasks);
        return cycle;
    }

}
