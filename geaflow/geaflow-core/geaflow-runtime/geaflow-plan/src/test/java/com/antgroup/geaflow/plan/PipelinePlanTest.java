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

package com.antgroup.geaflow.plan;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.pdata.PStreamSource;
import com.antgroup.geaflow.api.pdata.stream.PStream;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.WindowFactory;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.context.AbstractPipelineContext;
import com.antgroup.geaflow.core.graph.builder.ExecutionGraphBuilderTest;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.partitioner.impl.ForwardPartitioner;
import com.antgroup.geaflow.pdata.graph.view.IncGraphView;
import com.antgroup.geaflow.pdata.stream.window.WindowDataStream;
import com.antgroup.geaflow.pdata.stream.window.WindowStreamSource;
import com.antgroup.geaflow.plan.graph.PipelineEdge;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.plan.graph.PipelineVertex;
import com.antgroup.geaflow.plan.optimizer.PipelineGraphOptimizer;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import com.antgroup.geaflow.view.graph.PIncGraphView;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PipelinePlanTest extends BasePlanTest {

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @Test
    public void getSrcVertexIdTest() {
        List<Integer> ret = plan.getVertexInputVertexIds(7);
        Set<Integer> target = new HashSet<>();
        target.add(6);
        target.add(5);
        Assert.assertEquals(ret, target);

        ret = plan.getVertexInputVertexIds(4);
        target.clear();
        target.add(2);
        Assert.assertEquals(ret, target);
    }

    @Test
    public void testGetVertexInputEdges() {
        Map<Integer, Set<PipelineEdge>> map = plan.getVertexInputEdges();
        Set<PipelineEdge> target = new HashSet<>();
        target.add(new PipelineEdge(6, 3, 6, new ForwardPartitioner(), null));

        Assert.assertEquals(map.get(6), target);
    }

    @Test
    public void testGetVertexOutputEdges() {
        Map<Integer, Set<PipelineEdge>> map = plan.getVertexOutputEdges();
        Set<PipelineEdge> target = new HashSet<>();
        target.add(new PipelineEdge(2, 1, 2, new ForwardPartitioner(), null));
        target.add(new PipelineEdge(7, 1, 3, new ForwardPartitioner(), null));

        Assert.assertEquals(map.get(1), target);
    }

    @Test
    public void testTwoLayerReduce() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        PStreamSink sink = new WindowStreamSource<>(context,
            new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2))
            .keyBy(q -> q).reduce((oldValue, newValue) -> (oldValue)).keyBy(u -> u)
            .reduce((oldValue, newValue) -> (oldValue)).sink(p -> {});
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        // source->keyBy->reduce->keyBy->reduce->sink
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 6);
    }

    @Test
    public void testOneLayerReduce() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        PStreamSink sink = new WindowStreamSource<>(context,
            new CollectionSource(new ArrayList<>()), SizeTumblingWindow.of(2))
            .keyBy(q -> q).reduce((oldValue, newValue) -> (oldValue)).sink(p -> {
            });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        // source->keyBy->reduce->sink
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 4);
    }


    @Test
    public void testMultiOutput() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        WindowDataStream<Tuple<Long, Long>> ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)),
            SizeTumblingWindow.of(2));

        WindowDataStream<Tuple<Long, Long>> ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        PStream<Tuple<Long, Long>> ds = ds2.map(v -> v).withParallelism(3).filter(v -> v.f0 > 1L);
        PStreamSink sink1 = ds1.sink(p -> {});
        PStreamSink sink2 = ds.keyBy(p -> p).reduce((v1, v2) -> v1).sink(p -> {});
        when(context.getActions()).thenReturn(ImmutableList.of(sink1, sink2));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        // ds1_source->print;
        // ds2_source->map->filter keyby->reduce->print
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 8);
    }

    @Test
    public void testMaterialize() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<Integer> source1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowStream<IVertex<Integer, Double>> v = source1.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e = source1.map(i -> new ValueEdge<>(i, i, i));

        PStreamSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PStreamSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(4)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();
        PGraphView<Integer, Integer, Integer> fundGraphView =
            new IncGraphView<>(context, graphViewDesc);
        PIncGraphView<Integer, Integer, Integer> incGraphView =
            fundGraphView.appendGraph(
                vertices.window(WindowFactory.createSizeTumblingWindow(1)),
                edges.window(WindowFactory.createSizeTumblingWindow(1)));
        incGraphView.materialize();
        when(context.getActions()).thenReturn(ImmutableList.of(((IncGraphView)incGraphView).getMaterializedIncGraph()));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 3);

    }

    @Test
    public void testAllWindowCheckpointDuration() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        WindowStreamSource source = new WindowStreamSource(context,
            new CollectionSource<>(ImmutableList.of(1, 2, 3)), AllWindow.getInstance());
        PStreamSink sink = source
            .map(e -> Tuple.of(e, 1))
            .keyBy(v -> ((Tuple) v).f0)
            .reduce(new ExecutionGraphBuilderTest.CountFunc())
            .withParallelism(1)
            .sink(v -> {})
            .withParallelism(1);
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        Assert.assertEquals(context.getConfig().getLong(BATCH_NUMBER_PER_CHECKPOINT), 1);
    }
}
