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

import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.List;

public class UpdateEmitterRequest extends AbstractEmitterRequest {

    private final long pipelineId;
    private final String pipelineName;
    private final List<IOutputMessageBuffer<?, Shard>> outputBuffers;

    public UpdateEmitterRequest(int taskId,
                                long windowId,
                                long pipelineId,
                                String pipelineName,
                                List<IOutputMessageBuffer<?, Shard>> outputBuffers) {
        super(taskId, windowId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.outputBuffers = outputBuffers;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }

    public String getPipelineName() {
        return this.pipelineName;
    }

    public List<IOutputMessageBuffer<?, Shard>> getOutputBuffers() {
        return this.outputBuffers;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.UPDATE;
    }

}
