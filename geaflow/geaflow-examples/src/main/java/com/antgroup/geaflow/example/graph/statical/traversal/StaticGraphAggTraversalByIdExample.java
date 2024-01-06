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

package com.antgroup.geaflow.example.graph.statical.traversal;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphAggTraversalByIdExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticGraphAggTraversalByIdExample.class);

    public static final String REFERENCE_FILE_PATH = "data/reference/static_traversal_id_agg";
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/static_traversal_id_agg";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        StaticGraphAggTraversalByIdExample.submit(environment);
    }

    public static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Integer, Double>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Double> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                line -> {
                    String[] fields = line.split(",");
                    IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                    return Collections.singletonList(edge);
                }), AllWindow.getInstance())
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(2)
                .withBackend(BackendType.Memory)
                .build();

            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
            graphWindow.traversal(new StaticGraphAggTraversalAlgorithm(3))
                .start(108).withParallelism(1)
                .map(x -> String.format("%s,%s", x.getResponseId(), x.getResponse()))
                .sink(sink);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REFERENCE_FILE_PATH, RESULT_FILE_PATH);
    }
}
