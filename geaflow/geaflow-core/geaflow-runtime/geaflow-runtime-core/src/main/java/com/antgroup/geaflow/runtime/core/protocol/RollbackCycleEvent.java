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

import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rollback worker to specified batch.
 */
public class RollbackCycleEvent extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackCycleEvent.class);


    public RollbackCycleEvent(int workerId, int cycleId, long windowId) {
        super(workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        if (worker instanceof TransactionTrait) {
            LOGGER.info("worker do rollback {}", windowId);
            ((TransactionTrait) worker).rollback(windowId);
        }
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.ROLLBACK;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getIterationWindowId() {
        return windowId;
    }

    @Override
    public String toString() {
        return "RollbackCycleEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
