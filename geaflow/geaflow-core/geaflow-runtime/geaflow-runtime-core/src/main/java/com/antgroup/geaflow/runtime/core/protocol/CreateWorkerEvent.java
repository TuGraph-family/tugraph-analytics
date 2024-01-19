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
import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
import com.antgroup.geaflow.cluster.protocol.IHighAvailableEvent;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defined creating of the pipeline worker.
 */
public class CreateWorkerEvent implements IExecutableCommand, IHighAvailableEvent {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateWorkerEvent.class);

    private int workerId;
    private HighAvailableLevel haLevel;

    public CreateWorkerEvent(int workerId, HighAvailableLevel haLevel) {
        this.workerId = workerId;
        this.haLevel = haLevel;
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public void execute(ITaskContext context) {
        context.registerWorker(WorkerFactory.createWorker(context.getConfig()));
        LOGGER.info("create worker {} worker Id {}", context.getWorker(), workerId);
    }

    @Override
    public void interrupt() {

    }

    @Override
    public EventType getEventType() {
        return EventType.CREATE_WORKER;
    }

    @Override
    public String toString() {
        return "CreateWorkerEvent{"
            + "workerId=" + workerId
            + '}';
    }

    @Override
    public HighAvailableLevel getHaLevel() {
        return haLevel;
    }
}
