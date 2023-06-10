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

package com.antgroup.geaflow.example.graph.inc;

import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.graph.compute.IncVertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.WindowFactory;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncHistoricalGraph;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncTemporaryGraph;
import com.antgroup.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import com.antgroup.geaflow.view.graph.PIncGraphView;
import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IncGraphOperatorTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphOperatorTest.class);

    @Test
    public void testDynamicGraphVertexCentricComputeOp() {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), getClass().getSimpleName());
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/");

        Pipeline pipeline = PipelineFactory.buildPipeline(environment);

        int sourceParallelism = 1;
        int shardNum = 1;
        int iterationParallelism = 1;
        int sinkParallelism = 1;        //build graph view

        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(shardNum)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class,
                Object.class, ValueEdge.class, Object.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            List<IVertex<Integer, Integer>> vertexSource = Lists.newArrayList(
                new ValueVertex<>(1), new ValueVertex<>(2),
                new ValueVertex<>(3), new ValueVertex<>(4),
                new ValueVertex<>(5), new ValueVertex<>(6));

            List<IEdge<Integer, Integer>> edgeSource = Lists.newArrayList(
                new ValueEdge<>(1, 2),
                new ValueEdge<>(3, 4),
                new ValueEdge<>(5, 6));

            PWindowSource<IVertex<Integer, Integer>> vertices =
                pipelineTaskCxt.buildSource(new CollectionSource<>(vertexSource),
                        SizeTumblingWindow.of(2))
                    .window(WindowFactory.createSizeTumblingWindow(3))
                    .withParallelism(sourceParallelism);

            PWindowSource<IEdge<Integer, Integer>> edges =
                pipelineTaskCxt.buildSource(new CollectionSource<>(edgeSource),
                        SizeTumblingWindow.of(1))
                    .window(WindowFactory.createSizeTumblingWindow(3))
                    .withParallelism(sourceParallelism);

            PGraphView<Integer, Integer, Integer> fundGraphView =
                pipelineTaskCxt.getGraphView(graphName);

            PIncGraphView<Integer, Integer, Integer> incGraphView =
                fundGraphView.appendGraph(vertices, edges);

            incGraphView.incrementalCompute(new IncGraphAlgorithms(2))
                // incremental compute operator with 2 parallelism.
                .compute(iterationParallelism)
                .getVertices()
                .sink(v -> {})
                .withParallelism(sinkParallelism);

        });
        IPipelineResult result = pipeline.execute();
        result.get();
    }

    public static class IncGraphAlgorithms extends IncVertexCentricCompute<Integer, Integer, Integer, Integer> {

        public IncGraphAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> getIncComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction implements IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> {

        private IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext;

        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {
            this.graphContext = graphContext;
            TemporaryGraph<Integer, Integer, Integer> temporaryGraph =
                this.graphContext.getTemporaryGraph();
            TemporaryGraphCache temporaryGraphCache = (TemporaryGraphCache) getObjectField(temporaryGraph,
                IncTemporaryGraph.class, "temporaryGraphCache");
            Set<Integer> allEvolveVId = temporaryGraphCache.getAllEvolveVId();
            Assert.assertEquals(allEvolveVId.size(), 0);
        }

        @Override
        public void evolve(Integer vertexId, TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
            this.graphContext.sendMessage(999, 1);
            TemporaryGraphCache temporaryGraphCache = (TemporaryGraphCache) getObjectField(temporaryGraph,
                IncTemporaryGraph.class, "temporaryGraphCache");
            Set<Integer> allEvolveVId = temporaryGraphCache.getAllEvolveVId();
            Assert.assertEquals(allEvolveVId.size(), 2);

            long nestedWindowId = this.graphContext.getRuntimeContext().getWindowId();
            HistoricalGraph<Integer, Integer, Integer> historicalGraph = this.graphContext.getHistoricalGraph();
            GraphState graphState = (GraphState) getObjectField(historicalGraph,
                IncHistoricalGraph.class, "graphState");
            if (nestedWindowId == 2L) {
                List<Long> allVersions = graphState.dynamicGraph().V().getAllVersions(1);
                Assert.assertEquals(allVersions.size(), 1);
            }
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            int sum = 0;
            Assert.assertEquals((int) vertexId, 999);
            while (messageIterator.hasNext()) {
                sum += messageIterator.next();
            }
            Assert.assertEquals(sum, 2);
        }

        @Override
        public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
            IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();
            List<IEdge<Integer, Integer>> edges = graphContext.getTemporaryGraph().getEdges();
            if (vertex != null) {
                mutableGraph.addVertex(0, vertex);
                graphContext.collect(vertex);
            } else {
                LOGGER.info("not found vertex {} in temporaryGraph ", vertexId);
            }
            if (edges != null) {
                edges.stream().forEach(edge -> {
                    mutableGraph.addEdge(0, edge);
                });
            }
        }

    }

    private static Object getObjectField(Object instance, Class clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(instance);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
