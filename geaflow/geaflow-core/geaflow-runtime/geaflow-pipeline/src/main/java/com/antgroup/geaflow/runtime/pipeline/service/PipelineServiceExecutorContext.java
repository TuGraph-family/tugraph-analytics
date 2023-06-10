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

import com.antgroup.geaflow.runtime.pipeline.PipelineContext;
import com.antgroup.geaflow.runtime.pipeline.runner.PipelineRunner;

public class PipelineServiceExecutorContext {

    private String driverId;
    private long pipelineTaskId;
    private String pipelineTaskName;
    private PipelineContext pipelineContext;
    private PipelineRunner pipelineRunner;

    public PipelineServiceExecutorContext(String driverId,
                                          long pipelineTaskId,
                                          String pipelineTaskName,
                                          PipelineContext pipelineContext,
                                          PipelineRunner pipelineRunner) {
        this.driverId = driverId;
        this.pipelineTaskId = pipelineTaskId;
        this.pipelineTaskName = pipelineTaskName;
        this.pipelineContext = pipelineContext;
        this.pipelineRunner = pipelineRunner;
    }

    public String getDriverId() {
        return driverId;
    }

    public long getPipelineTaskId() {
        return pipelineTaskId;
    }

    public String getPipelineTaskName() {
        return pipelineTaskName;
    }

    public PipelineContext getPipelineContext() {
        return pipelineContext;
    }

    public PipelineRunner getPipelineRunner() {
        return pipelineRunner;
    }

}
