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

package com.antgroup.geaflow.runtime.pipeline.service;

import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.plan.PipelinePlanBuilder;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.runtime.pipeline.executor.PipelineExecutor;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineServiceExecutor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutor.class);

    private PipelineServiceExecutorContext serviceExecutorContext;

    public PipelineServiceExecutor(PipelineServiceExecutorContext serviceExecutorContext) {
        this.serviceExecutorContext = serviceExecutorContext;
    }

    public void start(PipelineService pipelineService) {
        // User pipeline Task.
        PipelineServiceContext serviceContext =
            new PipelineServiceContext(System.currentTimeMillis(),
                serviceExecutorContext.getPipelineContext());
        pipelineService.execute(serviceContext);

        PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
        // 1. Build pipeline graph plan.
        PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(serviceExecutorContext.getPipelineContext());

        // 2. Optimize pipeline graph plan.
        pipelinePlanBuilder.optimizePlan(serviceExecutorContext.getPipelineContext().getConfig());

        this.serviceExecutorContext.getPipelineRunner().runPipelineGraph(pipelineGraph, serviceExecutorContext);
    }
}
