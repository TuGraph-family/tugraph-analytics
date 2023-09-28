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
import com.antgroup.geaflow.cluster.worker.IAffinityWorker;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stash worker.
 */
public class StashWorkerEvent extends AbstractCleanCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(StashWorkerEvent.class);

    private final int taskId;

    public StashWorkerEvent(int workerId, int cycleId, long windowId, int taskId) {
        super(workerId, cycleId, windowId);
        this.taskId = taskId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        ExecutionTask executionTask = workerContext.getExecutionTask();
        workerContext.getEventMetrics().setFinishTime(System.currentTimeMillis());
        LOGGER.info("stash task {} {}/{} of {} {} : {}",
            executionTask.getTaskId(),
            executionTask.getIndex(),
            executionTask.getParallelism(),
            executionTask.getVertexId(),
            executionTask.getProcessor().toString(),
            workerContext.getEventMetrics());

        // Stash worker context.
        ((IAffinityWorker) worker).stash();

        this.emitterRunner.add(new CloseEmitterRequest(this.taskId, this.windowId));
        worker.close();
        LOGGER.info("stash worker context, taskId {}", ((WorkerContext) context).getTaskId());

        this.sendDoneEvent(workerContext.getDriverId(), EventType.CLEAN_CYCLE, null, true);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.STASH_WORKER;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getWindowId() {
        return windowId;
    }

    @Override
    public String toString() {
        return "StashWorkerEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
