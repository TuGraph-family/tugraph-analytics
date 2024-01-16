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
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutionGraphCycleSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testExecutionGraphWithCheckpointChileCycle() {
        long finishIterationId = 100;

        ExecutionGraphCycle graph = buildMockExecutionGraphCycle();
        RedoSchedulerContext parentContext = new RedoSchedulerContext(graph, null);
        parentContext.init(1);

        ExecutionNodeCycle iterationCycle = buildPipelineCycle(false, finishIterationId);
        graph.addCycle(iterationCycle);
        CheckpointSchedulerContext iterationContext = new CheckpointSchedulerContext(iterationCycle, parentContext);
        iterationContext.init();

        Assert.assertEquals(iterationContext.getCurrentIterationId(), 1);
        Assert.assertEquals(iterationContext.getFinishIterationId(), 100);

        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");

        // Recover case.
        CheckpointSchedulerContext loadedContext1 =
            (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(iterationCycle, parentContext);
        Assert.assertEquals(1, loadedContext1.getCurrentIterationId());

        loadedContext1.checkpoint(20);

        // Recover from checkpoint.
        CheckpointSchedulerContext loadedContext2 =
            (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(iterationCycle, parentContext);
        Assert.assertEquals(21, loadedContext2.getCurrentIterationId());
    }

    private ExecutionGraphCycle buildMockExecutionGraphCycle() {
        ClusterMetaStore.init(0, "driver-0", configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionGraphCycle(0, 0, 0, "test", 0,
            1, finishIterationId,
            configuration, "driver_id", 0);
    }

    protected ExecutionNodeCycle buildPipelineCycle(boolean isIterative, long iterationCount) {
        ClusterMetaStore.init(0, "driver-0", configuration);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(iterationCount);
        if (isIterative) {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
        } else {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        }
        ExecutionVertex vertex = new ExecutionVertex(1, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup, configuration, "driver_id", 0);
    }
}
