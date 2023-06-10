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

package com.antgroup.geaflow.example.window;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.stream.StreamUnionPipeline;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnionTest extends BaseTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(UnionTest.class);

    private Map<String, String> config;

    @BeforeMethod
    public void setUp() {
        config = new HashMap<>();
    }

    @Test
    public void testUnionWithAggSingleConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);

        StreamUnionPipeline pipeline = new StreamUnionPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void testUnionWithAggThreeConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        configuration.putAll(config);

        StreamUnionPipeline pipeline = new StreamUnionPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void testUnionWithAggThreeAndSinkThreeConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "3");
        configuration.putAll(config);

        StreamUnionPipeline pipeline = new StreamUnionPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void testUnionWithAggThreeAndSinkTwoConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "2");
        configuration.putAll(config);

        StreamUnionPipeline pipeline = new StreamUnionPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void testUnionWithAggThreeAndSinkFourConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);

        StreamUnionPipeline pipeline = new StreamUnionPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

}
