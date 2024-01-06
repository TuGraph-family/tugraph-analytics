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
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinishIterationEvent extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinishIterationEvent.class);

    public static final long END_OF_ITERATION_ID = 0;

    private int taskId;

    public FinishIterationEvent(int workerId, long windowId, int cycleId, int taskId) {
        super(workerId, cycleId, windowId);
        this.taskId = taskId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        worker.init(windowId);
        worker.finish(END_OF_ITERATION_ID);
        ((AbstractWorkerContext) this.context).getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.FINISH_ITERATION;
    }

    @Override
    public String toString() {
        return "FinishIterationEvent{"
                + "workerId=" + workerId
                + ", windowId=" + windowId
                + ", cycleId=" + cycleId
                + ", taskId=" + taskId
                + '}';
    }
}
