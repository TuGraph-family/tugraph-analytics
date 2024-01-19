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

package com.antgroup.geaflow.example.window.incremental;

import static com.antgroup.geaflow.example.config.ExampleConfigKeys.REDUCE_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SOURCE_PARALLELISM;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.stream.StreamWordCountPipeline;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class IncrWordCountTest extends BaseTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(IncrWordCountTest.class);


    @Test
    public void test() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        configuration.putAll(config);

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }

    @Test
    public void test1() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "2");
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
    public void test2() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
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
    public void test3() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
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
    public void test4() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "2");
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
    public void test5() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
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
    public void test6() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
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
