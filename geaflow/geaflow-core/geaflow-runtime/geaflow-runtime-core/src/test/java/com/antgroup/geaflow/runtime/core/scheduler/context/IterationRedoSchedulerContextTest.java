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
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IterationRedoSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testIterationFinishId() {
        long finishIterationId = 100;
        ExecutionNodeCycle cycle = buildMockCycle(false, finishIterationId);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext parentContext = new CheckpointSchedulerContext(cycle, null);
        parentContext.init(50);

        ExecutionNodeCycle iterationCycle = buildMockCycle(false, 5);
        IterationRedoSchedulerContext iterationContext = new IterationRedoSchedulerContext(iterationCycle, parentContext);
        iterationContext.init(20);

        Assert.assertEquals(iterationContext.getCurrentIterationId(), 1);
        Assert.assertEquals(iterationContext.getFinishIterationId(), 5);
    }

    protected ExecutionNodeCycle buildMockCycle(boolean isIterative, long iterationCount) {
        ClusterMetaStore.init(0, configuration);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(iterationCount);
        vertexGroup.getCycleGroupMeta().setIterative(isIterative);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, "test", vertexGroup, configuration, "driver_id");
    }

}
