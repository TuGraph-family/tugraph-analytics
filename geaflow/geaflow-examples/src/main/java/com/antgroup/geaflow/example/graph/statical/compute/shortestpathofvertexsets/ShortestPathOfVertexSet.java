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

package com.antgroup.geaflow.example.graph.statical.compute.shortestpathofvertexsets;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Triple;
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
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShortestPathOfVertexSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShortestPathOfVertexSet.class);

    private static final Set<Integer> SOURCE_ID = new HashSet<>(Arrays.asList(11342, 30957));

    public static final String REFERENCE_FILE_PATH = "data/reference/shortestpathvertex";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/shortestpathvertex";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
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

            FileSource<IVertex<Integer, Map<String, Map<Integer, Object>>>> vSource =
                new FileSource<>(GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserMapMap);
            PWindowStream<IVertex<Integer, Map<String, Map<Integer, Object>>>> vertices =
                 pipelineTaskCxt.buildSource(vSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            FileSource<IEdge<Integer, Map<String, Integer>>> eSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::edgeParserMap);
            PWindowStream<IEdge<Integer, Map<String, Integer>>> edges =
                pipelineTaskCxt.buildSource(eSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PWindowStream<IVertex<Integer, Map<String, Map<Integer, Object>>>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                .compute(new ShortestPathOfVertexSetAlgorithms(SOURCE_ID, 10))
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

    public static class ShortestPathOfVertexSetAlgorithms extends VertexCentricCompute<Integer,
        Map<String, Map<Integer, Object>>, Map<String, Integer>, Triple<Integer, Integer,
                List<Integer>>> {

        private final Set<Integer> sourceId;

        public ShortestPathOfVertexSetAlgorithms(Set<Integer> sourceId, long iterations) {
            super(iterations);
            this.sourceId = sourceId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Map<String, Map<Integer, Object>>, Map<String,
            Integer>, Triple<Integer, Integer, List<Integer>>> getComputeFunction() {
            return new ShortestPathOfVertexSetVCFunction(sourceId, EdgeDirection.OUT);
        }

        @Override
        public VertexCentricCombineFunction<Triple<Integer, Integer, List<Integer>>> getCombineFunction() {
            return null;
        }

    }

    public static class ShortestPathOfVertexSetVCFunction extends AbstractVcFunc<Integer,
        Map<String, Map<Integer, Object>>, Map<String, Integer>, Triple<Integer, Integer,
                List<Integer>>> {

        private static final String KEY_FIELD = "dis";

        private static final String PATH = "paths";

        private final Set<Integer> sourceId;
        private final EdgeDirection edgeType;

        public ShortestPathOfVertexSetVCFunction(Set<Integer> sourceId, EdgeDirection edgeType) {
            this.sourceId = sourceId;
            this.edgeType = edgeType;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Triple<Integer, Integer, List<Integer>>> messageIterator) {
            IVertex<Integer, Map<String, Map<Integer, Object>>> vertex = this.context.vertex().get();
            Map<String, Map<Integer, Object>> property = vertex.getValue();
            if (this.context.getIterationId() == 1) {
                Map<Integer, Object> dis = new HashMap<>(); //Map<Integer, Integer>
                Map<Integer, Object> path = new HashMap<>(); //Map<Integer, List<Integer>>
                for (Integer id : this.sourceId) {
                    if (vertex.getId().equals(id)) {
                        sendMessage(id, 0, new ArrayList<>(
                            Collections.singletonList(vertex.getId())));
                        dis.put(id, 0);
                        path.put(id, new ArrayList<>(Collections.singletonList(id)));
                    } else {
                        dis.put(id, Integer.MAX_VALUE);
                    }
                }
                property.put(KEY_FIELD, dis);
                property.put(PATH, path);
                this.context.setNewVertexValue(property);
            } else {
                Map<Integer, Integer> newDisMap = new HashMap<>(2);
                Map<Integer, List<Integer>> newPathMap = new HashMap<>(2);
                while (messageIterator.hasNext()) {
                    Triple<Integer, Integer, List<Integer>> meg = messageIterator.next();
                    if (meg.getF1() < newDisMap.getOrDefault(meg.getF0(), Integer.MAX_VALUE)) {
                        newDisMap.put(meg.getF0(), meg.getF1());
                        newPathMap.put(meg.getF0(), meg.getF2());
                    }
                }
                if (!newDisMap.isEmpty()) {
                    Map<Integer, Object> curDisMap = property.get(KEY_FIELD);
                    Map<Integer, Object> curPathMap = property.get(PATH);
                    for (Map.Entry<Integer, Integer> kv : newDisMap.entrySet()) {
                        if (kv.getValue() < (Integer) curDisMap.getOrDefault(kv.getKey(), Integer.MAX_VALUE)) {
                            curDisMap.put(kv.getKey(), kv.getValue());
                            List<Integer> tmp = new ArrayList<>(newPathMap.get(kv.getKey()));
                            tmp.add(vertex.getId());
                            curPathMap.put(kv.getKey(), tmp);
                            sendMessage(kv.getKey(), kv.getValue(), tmp);
                        }
                    }
                    Map<Integer, Object> dis = new HashMap<>(curDisMap);//Map<Integer, Integer>
                    Map<Integer, Object> path = new HashMap<>(curPathMap);//Map<Integer, List<Integer>>
                    property.put(KEY_FIELD, dis);
                    property.put(PATH, path);
                    this.context.setNewVertexValue(property);
                }
            }
        }

        private void sendMessage(Integer id, Integer distance, List<Integer> path) {
            switch (edgeType) {
                case IN:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getInEdges()) {
                        this.context.sendMessage(edge.getTargetId(), new Triple<>(id, distance + edge.getValue().get(KEY_FIELD), path));
                    }
                    break;
                case OUT:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getOutEdges()) {
                        this.context.sendMessage(edge.getTargetId(), new Triple<>(id, distance + edge.getValue().get(KEY_FIELD), path));
                    }
                    break;
                default:
                    break;
            }
        }

    }
}
