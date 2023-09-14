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
 *
 */

package com.antgroup.geaflow.runtime.core.scheduler.io;

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.response.ShardResult;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.core.graph.ExecutionEdge;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionTaskType;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.runtime.shuffle.ShardInputDesc;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.testng.Assert;

public class IoDescriptorBuilderTest extends TestCase {

    public void testBuildInputInfo() {

        int edgeId = 0;
        int parallelism = 3;
        ExecutionTask task = new ExecutionTask(1, 0, parallelism, 0, 0, 0);
        ExecutionEdge edge = new ExecutionEdge(null, edgeId, null, 0, 1, null);
        ExecutionNodeCycle cycle = buildMockCycle(new Configuration(), parallelism);

        CycleResultManager resultManager = new CycleResultManager();
        List<ISliceMeta> slices = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            slices.add(new PipelineSliceMeta(i, i, 0, 0, null));
        }
        ShardResult shards1 = new ShardResult(edgeId, CollectType.FORWARD, slices);
        resultManager.register(0, shards1);
        ShardResult shards2 = new ShardResult(edgeId, CollectType.FORWARD, slices);
        resultManager.register(0, shards2);

        IInputDesc input = IoDescriptorBuilder.buildInputInfo(task, edge, cycle, resultManager);
        ShardInputDesc shard = (ShardInputDesc) input;
        Assert.assertEquals(parallelism, shard.getInput().size());
        Assert.assertEquals(1, shard.getInput().get(0).getSlices().size());
    }

    private ExecutionNodeCycle buildMockCycle(Configuration configuration, int parallelism) {

        long finishIterationId = 5;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setIterative(false);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(parallelism);
        vertexGroup.getVertexMap().put(0, vertex);
        vertexGroup.putVertexId2InEdgeIds(0, new ArrayList<>());
        vertexGroup.putVertexId2OutEdgeIds(0, new ArrayList<>());

        List<ExecutionTask> headTasks = new ArrayList<>();
        List<ExecutionTask> tailTasks = new ArrayList<>();
        for (int i = 0; i < vertex.getParallelism(); i++) {
            ExecutionTask task = new ExecutionTask(i, i, vertex.getParallelism(), vertex.getParallelism(), vertex.getParallelism(), vertex.getVertexId());
            task.setExecutionTaskType(ExecutionTaskType.head);
            task.setWorkerInfo(new WorkerInfo());
            tailTasks.add(task);
            headTasks.add(task);
        }

        ExecutionNodeCycle cycle = new ExecutionNodeCycle(0, "test", vertexGroup, configuration, "driver_id");
        cycle.setCycleHeads(headTasks);
        cycle.setCycleTails(tailTasks);
        cycle.setTasks(headTasks);
        Map<Integer, List<ExecutionTask>> vertexIdToTasks = new HashMap<>();
        vertexIdToTasks.put(0, headTasks);
        cycle.setVertexIdToTasks(vertexIdToTasks);
        return cycle;
    }
}