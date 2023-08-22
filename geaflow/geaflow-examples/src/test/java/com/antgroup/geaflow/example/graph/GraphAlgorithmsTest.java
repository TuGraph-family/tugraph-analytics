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

package com.antgroup.geaflow.example.graph;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.graph.statical.compute.allshortestpath.AllShortestPath;
import com.antgroup.geaflow.example.graph.statical.compute.closenesscentrality.ClosenessCentrality;
import com.antgroup.geaflow.example.graph.statical.compute.clustercoefficient.ClusterCoefficient;
import com.antgroup.geaflow.example.graph.statical.compute.commonneighbors.CommonNeighbors;
import com.antgroup.geaflow.example.graph.statical.compute.kcore.KCore;
import com.antgroup.geaflow.example.graph.statical.compute.labelpropagation.LabelPropagation;
import com.antgroup.geaflow.example.graph.statical.compute.linkprediction.LinkPrediction;
import com.antgroup.geaflow.example.graph.statical.compute.npaths.NPaths;
import com.antgroup.geaflow.example.graph.statical.compute.pagerank.PageRank;
import com.antgroup.geaflow.example.graph.statical.compute.personalrank.PersonalRank;
import com.antgroup.geaflow.example.graph.statical.compute.shortestpath.ShortestPath;
import com.antgroup.geaflow.example.graph.statical.compute.shortestpathofvertexsets.ShortestPathOfVertexSet;
import com.antgroup.geaflow.example.graph.statical.compute.sssp.SSSP;
import com.antgroup.geaflow.example.graph.statical.compute.weakconnectedcomponents.WeakConnectedComponents;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class GraphAlgorithmsTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphAlgorithmsTest.class);

    private static final Map<String, String> TEST_CONFIG = new HashMap<>();
    static {
        TEST_CONFIG.put(ExampleConfigKeys.SOURCE_PARALLELISM.getKey(), String.valueOf(3));
        TEST_CONFIG.put(ExampleConfigKeys.ITERATOR_PARALLELISM.getKey(), String.valueOf(4));
        TEST_CONFIG.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), String.valueOf(5));
        TEST_CONFIG.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());
    }

    @Test
    public void weakConnectedComponentsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = WeakConnectedComponents.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        WeakConnectedComponents.validateResult();
    }


    @Test
    public void allShortestPathTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = AllShortestPath.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        AllShortestPath.validateResult();
    }

    @Test
    public void closenessCentralityTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = ClosenessCentrality.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ClosenessCentrality.validateResult();
    }

    @Test
    public void clusterCoefficientTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = ClusterCoefficient.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ClusterCoefficient.validateResult();
    }

    @Test
    public void commonNeighborsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = CommonNeighbors.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        CommonNeighbors.validateResult();
    }

    @Test
    public void kCoreTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = KCore.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        KCore.validateResult();
    }

    @Test
    public void labelPropagationTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = LabelPropagation.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        LabelPropagation.validateResult();
    }


    @Test
    public void linkPredictionTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = LinkPrediction.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        LinkPrediction.validateResult();
    }

    @Test
    public void nPathsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = NPaths.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        NPaths.validateResult();
    }

    @Test
    public void pageRankTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = PageRank.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        PageRank.validateResult();
    }

    @Test
    public void personalRankTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = PersonalRank.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        PersonalRank.validateResult();
    }

    @Test
    public void shortestPathTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = ShortestPath.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ShortestPath.validateResult();
    }

    @Test
    public void shortestPathOfVertexSetTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = ShortestPathOfVertexSet.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ShortestPathOfVertexSet.validateResult();
    }

    @Test
    public void SSSPTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);

        IPipelineResult result = SSSP.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        SSSP.validateResult();
    }

}
