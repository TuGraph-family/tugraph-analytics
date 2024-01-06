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

import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.cluster.response.ShardResult;
import com.antgroup.geaflow.core.graph.ExecutionEdge;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.testng.Assert;

public class DataExchangerTest extends TestCase {

    public void testBuildInput() {

        ExecutionVertex vertex = new ExecutionVertex(1, "test");
        int parallelism = 3;
        vertex.setParallelism(parallelism);
        int edgeId = 0;
        ExecutionEdge edge = new ExecutionEdge(null, edgeId, null, 0, 1, null);

        CycleResultManager resultManager = new CycleResultManager();
        ShardResult shards1 = new ShardResult(edgeId, CollectType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards1);
        ShardResult shards2 = new ShardResult(edgeId, CollectType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards2);

        Map<Integer, List<Shard>> result = DataExchanger.buildInput(vertex, edge, resultManager);
        Assert.assertEquals(parallelism, result.size());
        for (int i = 0; i < parallelism; i++) {
            Assert.assertEquals(2, result.get(i).size());
            Assert.assertEquals(i, result.get(i).get(0).getSlices().get(0).getSourceIndex());
            Assert.assertEquals(i, result.get(i).get(0).getSlices().get(0).getTargetIndex());
        }
    }

    public void testBuildInputWithResponse() {

        ExecutionVertex vertex = new ExecutionVertex(1, "test");
        int parallelism = 3;
        vertex.setParallelism(parallelism);
        int edgeId = 0;
        ExecutionEdge edge = new ExecutionEdge(null, edgeId, null, 0, 1, null);

        CycleResultManager resultManager = new CycleResultManager();
        ShardResult shards1 = new ShardResult(edgeId, CollectType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards1);

        ShardResult shards2 = new ShardResult(edgeId, CollectType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards2);

        ResponseResult response1 = new ResponseResult(edgeId, CollectType.RESPONSE, buildResponse());
        resultManager.register(1, response1);

        ResponseResult response2 = new ResponseResult(edgeId, CollectType.RESPONSE, buildResponse());
        resultManager.register(1, response2);

        // build input not include response.
        Map<Integer, List<Shard>> result = DataExchanger.buildInput(vertex, edge, resultManager);
        Assert.assertEquals(parallelism, result.size());
        for (int i = 0; i < parallelism; i++) {
            Assert.assertEquals(2, result.get(i).size());
            Assert.assertEquals(i, result.get(i).get(0).getSlices().get(0).getSourceIndex());
            Assert.assertEquals(i, result.get(i).get(0).getSlices().get(0).getTargetIndex());
        }
    }


    private List<ISliceMeta> buildSlices(int parallelism) {
        List<ISliceMeta> slices = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            slices.add(new PipelineSliceMeta(i, i, 0, 0, null));
        }
        return slices;
    }

    private List<Object> buildResponse() {
        return Arrays.asList(0);
    }

}