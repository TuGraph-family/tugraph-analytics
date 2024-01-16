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
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send from scheduler to cycle head task to launch one iteration of the cycle.
 */
public class LaunchSourceEvent extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(LaunchSourceEvent.class);

    public LaunchSourceEvent(long schedulerId, int workerId, int cycleId, long windowId) {
        super(schedulerId, workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        worker.init(windowId);
        if (!workerContext.isFinished()) {
            boolean hasData = (boolean) worker.process(null);
            if (!hasData) {
                workerContext.setFinished(true);
                this.sendDoneEvent(
                    workerContext.getDriverId(),
                    EventType.LAUNCH_SOURCE,
                    false,
                    false);
                LOGGER.info("source is finished at {}, workerId {}, taskId {}",
                    windowId, workerId, workerContext.getTaskId());
            }
        } else {
            this.sendDoneEvent(
                workerContext.getDriverId(),
                EventType.LAUNCH_SOURCE,
                false,
                false);
            LOGGER.info("source already finished, workerId {}, taskId {}",
                workerId,
                workerContext.getTaskId());
        }
        worker.finish(windowId);
        workerContext.getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
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

    @Override
    public EventType getEventType() {
        return EventType.LAUNCH_SOURCE;
    }

    @Override
    public String toString() {
        return "LaunchSourceEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
