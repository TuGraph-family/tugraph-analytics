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

package com.antgroup.geaflow.runtime.pipeline.executor;

import com.antgroup.geaflow.cluster.executor.IPipelineExecutor;
import com.antgroup.geaflow.cluster.executor.PipelineExecutorContext;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.runtime.pipeline.PipelineContext;
import com.antgroup.geaflow.runtime.pipeline.runner.PipelineRunner;
import com.antgroup.geaflow.runtime.pipeline.service.PipelineServiceExecutor;
import com.antgroup.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import com.antgroup.geaflow.runtime.pipeline.task.PipelineTaskExecutor;
import com.antgroup.geaflow.runtime.pipeline.task.PipelineTaskExecutorContext;
import com.antgroup.geaflow.view.IViewDesc;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineExecutor implements IPipelineExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutor.class);

    private static final String DEFAULT_PIPELINE_NAME = "PipelineTask";
    private PipelineRunner pipelineRunner;
    private PipelineExecutorContext executorContext;
    private List<IViewDesc> viewDescList;

    public void init(PipelineExecutorContext executorContext) {
        this.executorContext = executorContext;
        this.pipelineRunner = new PipelineRunner(executorContext.getEventDispatcher());
    }

    @Override
    public void register(List<IViewDesc> viewDescList) {
        this.viewDescList = viewDescList;
    }

    @Override
    public void runPipelineTask(PipelineTask pipelineTask, TaskCallBack taskCallBack) {
        int pipelineTaskId = executorContext.getIdGenerator().getAndIncrement();
        String pipelineTaskName = String.format("%s#%s", DEFAULT_PIPELINE_NAME, pipelineTaskId);
        LOGGER.info("run pipeline task {}", pipelineTaskName);

        PipelineContext pipelineContext = new PipelineContext(DEFAULT_PIPELINE_NAME,
            executorContext.getEnvConfig());
        this.viewDescList.stream().forEach(viewDesc -> pipelineContext.addView(viewDesc));

        PipelineTaskExecutorContext taskExecutorContext =
            new PipelineTaskExecutorContext(executorContext.getDriverId(),
                pipelineTaskId, pipelineTaskName, pipelineContext, pipelineRunner);
        PipelineTaskExecutor taskExecutor = new PipelineTaskExecutor(taskExecutorContext);
        taskExecutor.execute(pipelineTask, taskCallBack);
    }

    @Override
    public void startPipelineService(PipelineService pipelineService) {
        int pipelineTaskId = executorContext.getIdGenerator().getAndIncrement();
        String pipelineTaskName = String.format("%s#%s", DEFAULT_PIPELINE_NAME, pipelineTaskId);
        LOGGER.info("run pipeline task {}", pipelineTaskName);

        PipelineContext pipelineContext = new PipelineContext(DEFAULT_PIPELINE_NAME,
            executorContext.getEnvConfig());
        this.viewDescList.stream().forEach(viewDesc -> pipelineContext.addView(viewDesc));

        PipelineServiceExecutorContext pipelineServiceExecutorContext =
            new PipelineServiceExecutorContext(executorContext.getDriverId(),
                pipelineTaskId, pipelineTaskName, pipelineContext, pipelineRunner);
        PipelineServiceExecutor serviceExecutor =
            new PipelineServiceExecutor(pipelineServiceExecutorContext);
        serviceExecutor.start(pipelineService);
    }
}
