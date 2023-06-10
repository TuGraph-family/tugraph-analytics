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

import com.antgroup.geaflow.cluster.collector.CloseEmitterRequest;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;

/**
 * Clean worker runtime execution env, e.g. close shuffle reader/writer and close processor.
 * Reverse event of {@link InitCycleEvent}: {@link InitCycleEvent} for initialize env while {@link CleanCycleEvent} for clean env.
 */
public class CleanCycleEvent extends AbstractCleanCommand {

    public CleanCycleEvent(int workerId, int cycleId, long windowId) {
        super(workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        emitterRunner.add(new CloseEmitterRequest());
        worker.close();
        sendDoneEvent(cycleId, windowId, EventType.CLEAN_CYCLE);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.CLEAN_CYCLE;
    }

    public long getIterationWindowId() {
        return windowId;
    }

    @Override
    public String toString() {
        return "CleanCycleEvent{"
            + "workerId=" + workerId
            + ", windowId=" + windowId
            + '}';
    }
}
