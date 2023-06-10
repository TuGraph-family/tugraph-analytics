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

package com.antgroup.geaflow.pipeline;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.pipeline.scheduler.ISchedule;
import com.antgroup.geaflow.pipeline.task.PipelineTask;

public class SchedulerPipeline extends Pipeline {

    private ISchedule scheduler;

    public SchedulerPipeline(Environment environment) {
        super(environment);
    }

    public SchedulerPipeline schedule(PipelineTask pipelineTask) {
        submit(pipelineTask);
        return this;
    }

    public void with(ISchedule scheduler) {
        this.scheduler = scheduler;
    }
}
