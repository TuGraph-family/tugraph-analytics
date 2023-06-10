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

import static com.antgroup.geaflow.example.config.ExampleConfigKeys.REDUCE_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SOURCE_PARALLELISM;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import org.testng.annotations.Test;

public class WindowStreamWordCountTest extends BaseTest {

    @Test
    public void testSingleConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        WindowStreamWordCountPipeline pipeline = new WindowStreamWordCountPipeline();
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

    @Test
    public void testReduceTwoAndSinkFourConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        WindowStreamWordCountPipeline pipeline = new WindowStreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

    @Test
    public void testReduceOneAndSinkFourConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        WindowStreamWordCountPipeline pipeline = new WindowStreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "1");
        config.put(SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

    @Test
    public void testReduceTwoAndSinkTwoConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        WindowStreamWordCountPipeline pipeline = new WindowStreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "2");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

    @Test
    public void testReduceTwoAndSinkOneConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        WindowStreamWordCountPipeline pipeline = new WindowStreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "1");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

}
