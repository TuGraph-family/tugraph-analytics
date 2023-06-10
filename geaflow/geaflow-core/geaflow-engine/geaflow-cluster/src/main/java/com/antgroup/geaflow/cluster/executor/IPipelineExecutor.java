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

package com.antgroup.geaflow.cluster.executor;

import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.IViewDesc;
import java.util.List;

public interface IPipelineExecutor {

    /**
     * Init pipeline executor.
     */
    void init(PipelineExecutorContext executorContext);

    /**
     * Register view desc list.
     */
    void register(List<IViewDesc> viewDescList);

    /**
     * Trigger to run pipeline task.
     */
    void runPipelineTask(PipelineTask pipelineTask, TaskCallBack taskCallBack);

    /**
     * Trigger to start pipeline service.
     */
    void startPipelineService(PipelineService pipelineService);
}
