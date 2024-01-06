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

package com.antgroup.geaflow.example.graph.dynamic.traversal;

import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.RecoverableFileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.model.traversal.TraversalType.ResponseType;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import com.antgroup.geaflow.view.graph.PIncGraphView;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrGraphTraversalByStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphTraversalByStream.class);

    public static final String RESULT_FILE_PATH =
        "./target/tmp/data/result/incr_stream_graph_traversal";
    public static final String REF_FILE_PATH = "data/reference/incr_graph_traversal";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        //build graph view
        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(envConfig.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM))
            .withBackend(BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<IVertex<Integer, Integer>> vertices =
                    // extract vertex from edge file
                    pipelineTaskCxt.buildSource(new RecoverableFileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Integer> vertex1 = new ValueVertex<>(
                                Integer.valueOf(fields[0]), 1);
                            IVertex<Integer, Integer> vertex2 = new ValueVertex<>(
                                Integer.valueOf(fields[1]), 1);
                            return Arrays.asList(vertex1, vertex2);
                        }), SizeTumblingWindow.of(10000))
                        .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                PWindowSource<IEdge<Integer, Integer>> edges =
                    pipelineTaskCxt.buildSource( new RecoverableFileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), SizeTumblingWindow.of(5000));


                PGraphView<Integer, Integer, Integer> fundGraphView =
                    pipelineTaskCxt.getGraphView(graphName);

                PIncGraphView<Integer, Integer, Integer> incGraphView =
                    fundGraphView.appendGraph(vertices, edges);

                PWindowSource<ITraversalRequest<Integer>> triggerSource =
                    pipelineTaskCxt.buildSource(
                        new CollectionSource<>(getTraversalRequests()),
                        SizeTumblingWindow.of(1)
                    );

                int mapParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.MAP_PARALLELISM);
                int sinkParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SINK_PARALLELISM);
                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                incGraphView.incrementalTraversal(new IncGraphTraversalAlgorithms(3))
                    .start(triggerSource)
                    .map(res -> String.format("%s,%s", res.getResponseId(), res.getResponse()))
                    .withParallelism(mapParallelism)
                    .sink(sink)
                    .withParallelism(sinkParallelism);

            }
        });

        return pipeline.execute();
    }

    private static List<ITraversalRequest<Integer>> getTraversalRequests() {
        List<ITraversalRequest<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new VertexBeginTraversalRequest<>(3));
        }
        return list;
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }


    public static class IncGraphTraversalAlgorithms extends IncVertexCentricTraversal<Integer,
        Integer, Integer,
        Integer, Integer> {

        public IncGraphTraversalAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer> getIncTraversalFunction() {
            return new IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer>() {

                private IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext;

                @Override
                public void open(
                    IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer,
                        Integer> vertexCentricFuncContext) {
                    this.vertexCentricFuncContext = vertexCentricFuncContext;
                }

                @Override
                public void evolve(Integer vertexId,
                                   TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
                    LOGGER.debug("evolve vId:{}", vertexId);
                }

                @Override
                public void init(ITraversalRequest<Integer> traversalRequest) {
                    int requestId = traversalRequest.getVId();
                    this.vertexCentricFuncContext.sendMessageToNeighbors(requestId);
                }

                @Override
                public void finish() {

                }

                @Override
                public void close() {

                }

                @Override
                public void compute(Integer vertexId,
                                    Iterator<Integer> messageIterator) {
                    int sum = 0;
                    while (messageIterator.hasNext()) {
                        sum += messageIterator.next();
                    }
                    this.vertexCentricFuncContext.takeResponse(new TraversalResponse(vertexId, sum));
                    LOGGER.info("take response:{}-{}", vertexId, sum);
                }

                @Override
                public void finish(Integer vertexId,
                                   MutableGraph<Integer, Integer, Integer> mutableGraph) {
                    TemporaryGraph<Integer, Integer, Integer> temporaryGraph =
                        this.vertexCentricFuncContext.getTemporaryGraph();
                    IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
                    if (vertex != null) {
                        mutableGraph.addVertex(0, vertex);
                    }
                    List<IEdge<Integer, Integer>> edges = temporaryGraph.getEdges();
                    if (edges != null) {
                        for (IEdge<Integer, Integer> edge : edges) {
                            mutableGraph.addEdge(0, edge);
                        }
                    }
                }
            };
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    static class TraversalResponse implements ITraversalResponse<Integer> {

        private long responseId;
        private int value;

        public TraversalResponse(long responseId, int value) {
            this.responseId = responseId;
            this.value = value;
        }

        @Override
        public long getResponseId() {
            return responseId;
        }

        @Override
        public Integer getResponse() {
            return value;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }
    }

}
