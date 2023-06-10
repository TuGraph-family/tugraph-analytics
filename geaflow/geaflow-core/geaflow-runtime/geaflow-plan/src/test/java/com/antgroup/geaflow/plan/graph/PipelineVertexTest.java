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

package com.antgroup.geaflow.plan.graph;

import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.operator.impl.window.FilterOperator;
import com.antgroup.geaflow.operator.impl.window.SinkOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PipelineVertexTest {

    @Test
    public void testVertex() {
        PipelineVertex vertex1 = new PipelineVertex(1, new SinkOperator<>(), VertexType.sink, 2);
        Assert.assertTrue(vertex1.getType() == VertexType.sink);
        Assert.assertFalse(vertex1.isDuplication());
        Assert.assertNull(vertex1.getVertexMode());
        Assert.assertTrue(vertex1.getOperator() instanceof SinkOperator);
        vertex1.setOperator(new FilterOperator<>(new FilterFunction<Object>() {
            @Override
            public boolean filter(Object record) {
                return true;
            }
        }));
        Assert.assertTrue(vertex1.getOperator() instanceof FilterOperator);

        PipelineVertex vertex2 = new PipelineVertex(1, VertexType.sink, new SinkOperator<>(), VertexMode.append);
        Assert.assertTrue(vertex2.getType() == VertexType.sink);
        Assert.assertFalse(vertex2.isDuplication());
        Assert.assertTrue(vertex2.getVertexMode() == VertexMode.append);
        Assert.assertTrue(vertex2.getOperator() instanceof SinkOperator);
        vertex2.setDuplication();
        Assert.assertTrue(vertex2.isDuplication());

        Assert.assertTrue(vertex1.equals(vertex2));

        vertex1.setVertexId(2);
        Assert.assertFalse(vertex1.equals(vertex2));
    }
}
