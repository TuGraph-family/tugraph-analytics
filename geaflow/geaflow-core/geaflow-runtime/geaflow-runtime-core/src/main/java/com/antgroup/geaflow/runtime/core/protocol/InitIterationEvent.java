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

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitIterationEvent<T> extends AbstractInitCommand {

    private IoDescriptor ioDescriptor;
    private final long pipelineId;
    private final String pipelineName;

    public InitIterationEvent(int workerId, int cycleId, long iterationId, long pipelineId, String pipelineName) {
        super(workerId, cycleId, iterationId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        initFetchRequest(ioDescriptor, pipelineId, pipelineName);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.INIT_ITERATION;
    }

    public void setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
    }

    @Override
    public String toString() {
        return "InitIterationEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", iterationId=" + windowId
            + '}';
    }
}
