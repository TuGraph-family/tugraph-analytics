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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.List;

public class InitEmitterRequest extends AbstractEmitterRequest {

    private final Configuration configuration;
    private final long pipelineId;
    private final String pipelineName;
    private final TaskArgs taskArgs;
    private final OutputDescriptor outputDescriptor;
    protected final List<IOutputMessageBuffer<?, Shard>> outputBuffers;

    public InitEmitterRequest(Configuration configuration,
                              long windowId,
                              long pipelineId,
                              String pipelineName,
                              TaskArgs taskArgs,
                              OutputDescriptor outputDescriptor,
                              List<IOutputMessageBuffer<?, Shard>> outputBuffers) {
        super(taskArgs.getTaskId(), windowId);
        this.configuration = configuration;
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.taskArgs = taskArgs;
        this.outputDescriptor = outputDescriptor;
        this.outputBuffers = outputBuffers;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }

    public String getPipelineName() {
        return this.pipelineName;
    }

    public TaskArgs getTaskArgs() {
        return this.taskArgs;
    }

    public OutputDescriptor getOutputDescriptor() {
        return this.outputDescriptor;
    }

    public List<IOutputMessageBuffer<?, Shard>> getOutputBuffers() {
        return this.outputBuffers;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.INIT;
    }

}
