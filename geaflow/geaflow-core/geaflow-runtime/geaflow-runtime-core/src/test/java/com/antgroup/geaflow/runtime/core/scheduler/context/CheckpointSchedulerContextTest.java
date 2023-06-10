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
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CheckpointSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testRestartContext() {
        long finishIterationId = 100;
        String testName = "testName";
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.setName(testName);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext context = new CheckpointSchedulerContext(cycle, null);

        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
        CheckpointSchedulerContext loaded = (CheckpointSchedulerContext) CheckpointSchedulerContext.build(() -> context);
        loaded.getCurrentIterationId();
        Assert.assertEquals(1, loaded.getCurrentIterationId());

        long checkpointId = 10;
        loaded.checkpoint(checkpointId);
        Assert.assertNotNull(ClusterMetaStore.getInstance().getCycle());

        // mock restart job
        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test2");
        ClusterMetaStore.init(0, configuration);

        CheckpointSchedulerContext loaded2 = (CheckpointSchedulerContext) CheckpointSchedulerContext.build(() -> context);
        Assert.assertEquals(checkpointId + 1, loaded2.getCurrentIterationId());
        Assert.assertNull(ClusterMetaStore.getInstance().getCycle());

    }


    private ExecutionNodeCycle buildMockCycle(boolean isIterative) {
        ClusterMetaStore.init(0, configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setIterative(isIterative);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, "test", vertexGroup, configuration, "driver_id");
    }

}
