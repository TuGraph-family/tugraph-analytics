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
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContextManager;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;

public class CleanEnvEvent extends AbstractCleanCommand {

    private final long pipelineId;
    private final String driverId;

    public CleanEnvEvent(int workerId, int cycleId, long iterationId, long pipelineId, String driverId) {
        super(workerId, cycleId, iterationId);
        this.pipelineId = pipelineId;
        this.driverId = driverId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        ShuffleDataManager.getInstance().release(pipelineId);
        WorkerContextManager.clear();
        sendDoneEvent(cycleId, windowId, EventType.CLEAN_ENV);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.CLEAN_ENV;
    }

    public int getCycleId() {
        return cycleId;
    }

    public void setIterationId(int iterationId) {
        this.windowId = iterationId;
    }

    @Override
    protected void sendDoneEvent(int cycleId, long windowId, EventType eventType) {
        DoneEvent doneEvent = new DoneEvent(cycleId, windowId, 0, eventType);
        RpcClient.getInstance().processPipeline(driverId, doneEvent);
    }

    @Override
    public String toString() {
        return "CleanEnvEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + '}';
    }
}
