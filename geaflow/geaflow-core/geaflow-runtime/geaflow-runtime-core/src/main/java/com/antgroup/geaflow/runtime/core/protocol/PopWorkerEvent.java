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
import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.cluster.worker.IAffinityWorker;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pop worker from cache and reuse context.
 */
public class PopWorkerEvent extends AbstractInitCommand implements IExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(PopWorkerEvent.class);

    private long pipelineId;
    private String pipelineName;
    private IoDescriptor ioDescriptor;
    private int taskId;

    public PopWorkerEvent(int workerId, int cycleId, long windowId,
                          long pipelineId, String pipelineName, int taskId) {
        super(workerId, cycleId, windowId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.taskId = taskId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        LOGGER.info("reuse worker context, taskId {}", taskId);
        AbstractWorkerContext popWorkerContext = new WorkerContext(taskContext);
        popWorkerContext.setPipelineId(pipelineId);
        popWorkerContext.setPipelineName(pipelineName);
        popWorkerContext.setWindowId(windowId);
        popWorkerContext.setTaskId(taskId);

        ((IAffinityWorker) worker).pop(popWorkerContext);
        context = worker.getWorkerContext();
        emitterRunner.add(new CloseEmitterRequest());

        AbstractWorkerContext workerContext = (AbstractWorkerContext) context;
        if (!(ExecutionTaskUtils.isCycleHead(workerContext.getExecutionTask())
            && ioDescriptor.getInputTaskNum() == 0)) {
            initFetchRequest(ioDescriptor, pipelineId, pipelineName);
        }
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.POP_WORKER;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getWindowId() {
        return windowId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public void setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
    }

    @Override
    public String toString() {
        return "PopWorkerEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + '}';
    }
}
