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

package com.antgroup.geaflow.example.graph.inc.traversal;

import static com.antgroup.geaflow.example.config.ExampleConfigKeys.ITERATOR_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.MAP_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SOURCE_PARALLELISM;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.graph.dynamic.traversal.IncrGraphAggTraversalAll;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncGraphAggTraversalAllTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphAggTraversalAllTest.class);

    @BeforeMethod
    public void setUp() {
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), getClass().getSimpleName() + "-" + System.currentTimeMillis());
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/");
        String path = config.get(FileConfigKeys.ROOT.getKey()) + config.get(ExecutionConfigKeys.JOB_APP_NAME.getKey());
        FileUtils.deleteQuietly(new File(path));
    }

    @Test
    public void test1ShardWithSingleConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCMapOneSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCFourMapSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(MAP_PARALLELISM.getKey(), String.valueOf(4));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCMapFourSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test1ShardWithOneSourceVCMapFourSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(1));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCOneMapTwoSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(2));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCOneMapTwoSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(2));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCMapSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        IncrGraphAggTraversalAll pipeline = new IncrGraphAggTraversalAll();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(1));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }
}
