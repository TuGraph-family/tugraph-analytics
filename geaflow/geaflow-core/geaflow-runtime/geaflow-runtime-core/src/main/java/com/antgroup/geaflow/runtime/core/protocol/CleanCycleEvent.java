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
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.common.utils.GcUtil;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean worker runtime execution env, e.g. close shuffle reader/writer and close processor.
 * Reverse event of {@link InitCycleEvent}: {@link InitCycleEvent} for initialize env while {@link CleanCycleEvent} for clean env.
 */
public class CleanCycleEvent extends AbstractCleanCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanCycleEvent.class);

    public CleanCycleEvent(int workerId, int cycleId, long windowId) {
        super(workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        ExecutionTask executionTask = workerContext.getExecutionTask();
        this.emitterRunner.add(new CloseEmitterRequest(executionTask.getTaskId(), this.windowId));
        this.worker.close();
        EventMetrics eventMetrics = ((AbstractWorkerContext) this.context).getEventMetrics();
        eventMetrics.setFinishTime(System.currentTimeMillis());
        eventMetrics.setFinishGcTs(GcUtil.computeCurrentTotalGcTime());
        LOGGER.info("clean task {} {}/{} of {} {} : {}",
            executionTask.getTaskId(),
            executionTask.getIndex(),
            executionTask.getParallelism(),
            executionTask.getVertexId(),
            executionTask.getProcessor().toString(),
            eventMetrics);
        this.sendDoneEvent(workerContext.getDriverId(), EventType.CLEAN_CYCLE, null, true);
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
