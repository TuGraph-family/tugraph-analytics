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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.base.BaseTest;
import com.antgroup.geaflow.example.stream.StreamWordCountCallBackPipeline;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class IncrWordCountCallBackTest extends BaseTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(IncrWordCountCallBackTest.class);

    @Test
    public void testTaskCallBack() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        configuration.putAll(config);

        StreamWordCountCallBackPipeline pipeline = new StreamWordCountCallBackPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }
}
