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

package com.antgroup.geaflow.example.graph.statical.compute.sssp;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSP {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSSP.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/sssp";
    public static final String REF_FILE_PATH = "data/reference/sssp";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Integer, Integer>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Integer> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new FileSource<>(
                "data/input/email_edge", line -> {
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
            PGraphWindow<Integer, Integer, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);
            SinkFunction<IVertex<Integer, Integer>> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new SSSPAlgorithm(1, 10))
                .compute(iterationParallelism)
                .getVertices()
                .sink(sink)
                .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));

        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }

    public static class SSSPAlgorithm extends VertexCentricCompute<Integer, Integer, Integer, Integer> {

        private final int srcId;

        public SSSPAlgorithm(int srcId, long iterations) {
            super(iterations);
            this.srcId = srcId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Integer, Integer, Integer> getComputeFunction() {
            return new SSSP.SSSPVertexCentricComputeFunction(srcId);
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class SSSPVertexCentricComputeFunction extends AbstractVcFunc<Integer, Integer, Integer, Integer> {

        private final int srcId;

        public SSSPVertexCentricComputeFunction(int srcId) {
            this.srcId = srcId;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Integer> messageIterator) {
            IVertex<Integer, Integer> vertex = this.context.vertex().get();
            int minDistance = vertex.getId() == srcId ? 0 : Integer.MAX_VALUE;
            if (messageIterator != null) {
                while (messageIterator.hasNext()) {
                    Integer value = messageIterator.next();
                    minDistance = Math.min(minDistance, value);
                }
            }
            if (minDistance < vertex.getValue()) {
                this.context.setNewVertexValue(minDistance);
                for (IEdge<Integer, Integer> edge : this.context.edges().getOutEdges()) {
                    this.context.sendMessage(edge.getTargetId(), minDistance + edge.getValue());
                }
            }
        }

    }
}
