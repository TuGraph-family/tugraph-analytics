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

package com.antgroup.geaflow.example.graph.statical.compute.pagerank;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRank {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageRank.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/pagerank";
    public static final String REF_FILE_PATH = "data/reference/pagerank";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = PageRank.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Integer, Double>> prVertices = pipelineTaskCxt.buildSource(
                    new FileSource<>("data/input/email_vertex", line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Double> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(
                    new FileSource<>("data/input/email_edge", line -> {
                        String[] fields = line.split(",");
                        IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                        return Collections.singletonList(edge);
                    }), AllWindow.getInstance())
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Integer, Double>> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new PRAlgorithms(10, 0.85))
                .compute(iterationParallelism)
                .getVertices()
                .map(e -> {
                    double value = Double.parseDouble(String.format("%.2f", e.getValue()));
                    return e.withValue(value);
                })
                .sink(sink)
                .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }

    public static class PRAlgorithms extends VertexCentricCompute<Integer, Double, Integer, Double> {

        private double alpha;

        public PRAlgorithms(long iterations, double alpha) {
            super(iterations);
            this.alpha = alpha;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Double> getComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

        private class PRVertexCentricComputeFunction extends AbstractVcFunc<Integer, Double, Integer, Double> {

            @Override
            public void compute(Integer vertexId,
                                Iterator<Double> messageIterator) {
                IVertex<Integer, Double> vertex = this.context.vertex().get();
                List<IEdge<Integer, Integer>> outEdges = context.edges().getOutEdges();
                if (this.context.getIterationId() == 1) {
                    if (!outEdges.isEmpty()) {
                        this.context.sendMessageToNeighbors(vertex.getValue() / outEdges.size());
                    }

                } else {
                    double sum = 0;
                    while (messageIterator.hasNext()) {
                        double value = messageIterator.next();
                        sum += value;
                    }
                    double pr = sum * alpha + (1 - alpha);
                    this.context.setNewVertexValue(pr);

                    if (!outEdges.isEmpty()) {
                        this.context.sendMessageToNeighbors(pr / outEdges.size());
                    }
                }
            }
        }
    }


}
