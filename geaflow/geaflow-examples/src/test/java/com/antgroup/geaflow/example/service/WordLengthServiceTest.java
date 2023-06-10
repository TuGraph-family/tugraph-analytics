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

package com.antgroup.geaflow.example.service;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WordLengthServiceTest {

    @BeforeMethod
    public void setUp() {
        ClusterMetaStore.close();
    }

    @Test
    public void testWordLength() throws Exception {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        WordLengthService wordLengthService = new WordLengthService();
        IPipelineResult result = wordLengthService.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        environment.shutdown();
    }

    @Test
    public void testWordLengthSourceMultiParallelism() throws Exception {
        Environment environment = EnvironmentFactory.onLocalEnvironment();

        Map<String, String> config = new HashMap<>();
        config.put(ExampleConfigKeys.SOURCE_PARALLELISM.getKey(), "3");
        environment.getEnvironmentContext().withConfig(config);
        WordLengthService wordLengthService = new WordLengthService();
        IPipelineResult result = wordLengthService.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        environment.shutdown();
    }

}
