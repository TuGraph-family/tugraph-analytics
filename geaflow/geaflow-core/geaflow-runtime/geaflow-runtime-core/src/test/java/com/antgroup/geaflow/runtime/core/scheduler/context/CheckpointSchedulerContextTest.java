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

package com.antgroup.geaflow.runtime.core.scheduler.context;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.core.graph.CycleGroupType;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CheckpointSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testNewContext() {
        long finishIterationId = 100;
        String testName = "testName";
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.setName(testName);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);

        CheckpointSchedulerContext context = (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(cycle, null);

        Assert.assertEquals(1, context.getCurrentIterationId());
        Assert.assertEquals(finishIterationId, context.getFinishIterationId());

    }

    @Test
    public void testRestartContext() {
        long finishIterationId = 100;
        String testName = "testName";
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.setName(testName);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext context = (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(cycle, null);

        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
        ClusterMetaStore.init(0, "driver-0", configuration);

        CheckpointSchedulerContext loaded =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), () -> context);
        Assert.assertEquals(1, loaded.getCurrentIterationId());

        long checkpointId = 10;
        loaded.checkpoint(checkpointId);
        Assert.assertNotNull(ClusterMetaStore.getInstance().getCycle(context.getCycle().getPipelineTaskId()));

        // Mock restart job.
        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test2");
        ClusterMetaStore.init(0, "driver-0", configuration);

        CheckpointSchedulerContext loaded2 =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), () ->
            CycleSchedulerContextFactory.create(cycle, null));
        Assert.assertEquals(checkpointId + 1, loaded2.getCurrentIterationId());
    }

    @Test
    public void testFailoverRecover() {
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
        long finishIterationId = 100;
        String testName = "testName";
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.setName(testName);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);

        CheckpointSchedulerContext context = (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(cycle, null);
        long checkpointId = 10;
        context.checkpoint(checkpointId);

        ClusterMetaStore.close();
        ClusterMetaStore.init(0, "driver-0", configuration);

        CheckpointSchedulerContext loaded =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), () -> context);

        Assert.assertEquals(checkpointId + 1, loaded.getCurrentIterationId());
        Assert.assertEquals(finishIterationId, context.getFinishIterationId());
    }

    private ExecutionNodeCycle buildMockCycle(boolean isIterative) {
        ClusterMetaStore.init(0, "driver-0", configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        if (isIterative) {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
        } else {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        }
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup, configuration, "driver_id", 0);
    }

}
