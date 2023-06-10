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

import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.pdata.PWindowCollect;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.service.IPipelineServiceContext;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.google.common.collect.Lists;

public class WordLengthService {

    public IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.start(new PipelineService() {
            @Override
            public void execute(IPipelineServiceContext pipelineServiceContext) {
                int sourceParallelism = pipelineServiceContext.getConfig().getInteger(ExampleConfigKeys.SOURCE_PARALLELISM);
                String word = (String) pipelineServiceContext.getRequest();
                word = "test";
                PWindowSource<String> windowSource = pipelineServiceContext
                    .buildSource(new CollectionSource<>(Lists.newArrayList(word)), AllWindow.getInstance())
                    .withParallelism(sourceParallelism);
                PWindowCollect collect = windowSource.map(x -> x.length()).collect();
                pipelineServiceContext.response(collect);
            }
        });
        return pipeline.execute();
    }
}
