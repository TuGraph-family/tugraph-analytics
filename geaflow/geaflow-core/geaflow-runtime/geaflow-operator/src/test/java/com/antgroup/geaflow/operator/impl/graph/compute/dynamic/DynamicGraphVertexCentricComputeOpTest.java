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

package com.antgroup.geaflow.operator.impl.graph.compute.dynamic;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.graph.compute.IncVertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.utils.ReflectionUtil;
import com.antgroup.geaflow.context.AbstractRuntimeContext;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DynamicGraphVertexCentricComputeOpTest {

    @Test
    public void testUpdateWindowId() {
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView("test")
            .withShardNum(1)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();

        DynamicGraphVertexCentricComputeOp operator = new DynamicGraphVertexCentricComputeOp(graphViewDesc, new IncVertexCentricCompute(5) {
            @Override
            public IncVertexCentricComputeFunction getIncComputeFunction() {
                return mock(IncVertexCentricComputeFunction.class);
            }

            @Override
            public VertexCentricCombineFunction getCombineFunction() {
                return null;
            }
        });
        ((AbstractOperator)operator).getOpArgs().setOpName("test");

        List<ICollector> collectors = new ArrayList<>();
        ICollector collector = mock(ICollector.class);
        when(collector.getId()).thenReturn(0);
        when(collector.getTag()).thenReturn("tag");

        collectors.add(collector);
        collectors.add(collector);
        long startWindowId = 0;
        Operator.OpContext context = new AbstractOperator.DefaultOpContext(collectors, new TestRuntimeContext());
        operator.open(context);

        Assert.assertEquals(startWindowId, ReflectionUtil.getField(operator, "windowId"));

        operator.initIteration(2);
        Assert.assertEquals(startWindowId, ReflectionUtil.getField(operator, "windowId"));
        Assert.assertEquals(startWindowId, ((RuntimeContext) ReflectionUtil.getField(operator, "runtimeContext")).getWindowId());

        ((AbstractRuntimeContext) context.getRuntimeContext()).updateWindowId(3L);
        operator.initIteration(1);

        Assert.assertEquals(3L, ReflectionUtil.getField(operator, "windowId"));
        Assert.assertEquals(3L, ((RuntimeContext) ReflectionUtil.getField(operator, "runtimeContext")).getWindowId());
    }

    public class TestRuntimeContext extends AbstractRuntimeContext {

        public TestRuntimeContext() {
            super(new Configuration());
        }

        @Override
        public long getPipelineId() {
            return 0;
        }

        @Override
        public String getPipelineName() {
            return null;
        }

        @Override
        public TaskArgs getTaskArgs() {
            return new TaskArgs(0, 0, "test", 1, 1);
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public RuntimeContext clone(Map<String, String> opConfig) {
            return new TestRuntimeContext();
        }

        @Override
        public long getWindowId() {
            return windowId;
        }

        @Override
        public MetricGroup getMetric() {
            Configuration config = new Configuration();
            config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
            return MetricGroupRegistry.getInstance(config).getMetricGroup();
        }

    }
}
