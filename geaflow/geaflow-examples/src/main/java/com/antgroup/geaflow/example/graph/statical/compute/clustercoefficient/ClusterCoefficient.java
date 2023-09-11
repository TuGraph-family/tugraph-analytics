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

package com.antgroup.geaflow.example.graph.statical.compute.clustercoefficient;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.data.GraphDataSet;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.example.util.VertexEdgeParser;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.io.IOException;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterCoefficient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterCoefficient.class);

    public static final int SOURCE_ID = 11342;

    public static final String REFERENCE_FILE_PATH = "data/reference/clustercoefficient";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/clustercoefficient";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = ClusterCoefficient.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
        ResultValidator.cleanResult(RESULT_FILE_DIR);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_DIR);

        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration config = pipelineTaskCxt.getConfig();
            int sourceParallelism = config.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM);
            int iterationParallelism = config.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            int sinkParallelism = config.getInteger(ExampleConfigKeys.SINK_PARALLELISM);
            LOGGER.info("with {} {} {}", sourceParallelism, iterationParallelism, sinkParallelism);

            FileSource<IVertex<Integer, Double>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserDouble);
            PWindowStream<IVertex<Integer, Double>> vertices =
                pipelineTaskCxt.buildSource(vSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            FileSource<IEdge<Integer, Integer>> eSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::edgeParserInteger);
            PWindowStream<IEdge<Integer, Integer>> edges =
                pipelineTaskCxt.buildSource(eSource, AllWindow.getInstance()).withParallelism(sourceParallelism);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PWindowStream<IVertex<Integer, Double>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                .compute(new ClusterCoefficientAlgorithm(SOURCE_ID, 10))
                .compute(iterationParallelism)
                .getVertices();

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(config);
            result.map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                .sink(sink).withParallelism(sinkParallelism);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REFERENCE_FILE_PATH, RESULT_FILE_DIR);
    }

    public static class ClusterCoefficientAlgorithm extends VertexCentricCompute<Integer, Double, Integer, Integer> {

        private final int srcId;

        public ClusterCoefficientAlgorithm(int srcId, long iterations) {
            super(iterations);
            this.srcId = srcId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Integer> getComputeFunction() {
            return new ClusterCoefficientVCCFunction(this.srcId);
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class ClusterCoefficientVCCFunction extends AbstractVcFunc<Integer, Double, Integer, Integer> {

        private final int srcId;

        public ClusterCoefficientVCCFunction(int srcId) {
            this.srcId = srcId;
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            IVertex<Integer, Double> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1L) {
                if (vertex.getId().equals(this.srcId)) {
                    sendMessage(1);
                }
            } else if (this.context.getIterationId() == 2L) {
                sendMessage(1);
            } else if (this.context.getIterationId() == 3L) {
                if (!vertex.getId().equals(this.srcId)) {
                    int meg = 0;
                    while (messageIterator.hasNext()) {
                        messageIterator.next();
                        meg++;
                    }
                    sendMessage(meg);
                }
            } else if (this.context.getIterationId() == 4L) {
                if (vertex.getId().equals(this.srcId)) {
                    int edgeNum = 0;
                    while (messageIterator.hasNext()) {
                        int value = messageIterator.next();
                        edgeNum += value;
                    }
                    int degree = this.context.edges().getInEdges().size() + this.context.edges().getOutEdges().size();
                    this.context.setNewVertexValue(((double) edgeNum) / (degree * (degree - 1)));
                }
            }
        }

        private void sendMessage(int meg) {
            for (IEdge<Integer, Integer> edge : this.context.edges().getInEdges()) {
                this.context.sendMessage(edge.getTargetId(), meg);
            }
            for (IEdge<Integer, Integer> edge : this.context.edges().getOutEdges()) {
                this.context.sendMessage(edge.getTargetId(), meg);
            }
        }

    }

}
