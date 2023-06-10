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

public class LoadGraphProcessEvent extends AbstractIterationComputeCommand {

    public LoadGraphProcessEvent(int workerId, int cycleId, long windowId, long fetchWindowId, long fetchCount) {
        super(workerId, cycleId, windowId, fetchWindowId, fetchCount);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getIterationWindowId() {
        return windowId;
    }

    public long getFetchWindowId() {
        return fetchWindowId;
    }

    @Override
    public EventType getEventType() {
        return EventType.PRE_GRAPH_PROCESS;
    }

    @Override
    public String toString() {
        return "LoadGraphProcessEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", fetchWindowId=" + fetchWindowId
            + '}';
    }
}
