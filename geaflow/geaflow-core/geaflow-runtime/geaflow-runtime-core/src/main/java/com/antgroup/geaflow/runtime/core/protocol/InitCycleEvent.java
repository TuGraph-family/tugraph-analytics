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

import com.antgroup.geaflow.cluster.collector.InitEmitterRequest;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEventContext;
import com.antgroup.geaflow.cluster.protocol.IHighAvailableEvent;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.context.EventContext;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitCycleEvent extends AbstractInitCommand implements IHighAvailableEvent {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitCycleEvent.class);

    private long pipelineId;
    private String pipelineName;
    private ExecutionTask task;
    private IoDescriptor ioDescriptor;
    private String driverId;
    private HighAvailableLevel haLevel;
    private long iterationWindowId;

    public InitCycleEvent(int workerId, int cycleId, long iterationId,
                          long pipelineId, String pipelineName,
                          ExecutionTask task, HighAvailableLevel haLevel,
                          long windowId) {
        super(workerId, cycleId, iterationId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.task = task;
        this.haLevel = haLevel;
        this.iterationWindowId = windowId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        AbstractWorkerContext workerContext = new WorkerContext(taskContext);
        context = workerContext;
        IEventContext eventContext = EventContext.builder()
            .withExecutionTask(task)
            .withDriverId(driverId)
            .withCycleId(cycleId)
            .withIoDescriptor(ioDescriptor)
            .withPipelineId(pipelineId)
            .withCurrentWindowId(windowId)
            .withPipelineName(pipelineName)
            .withWindowId(iterationWindowId)
            .build();
        workerContext.init(eventContext);
        if (!(ExecutionTaskUtils.isCycleHead(workerContext.getExecutionTask())
                && ioDescriptor.getInputTaskNum() == 0)) {
            initFetchRequest(ioDescriptor, pipelineId, pipelineName);
        }
        if (ioDescriptor.getOutputDescriptor() != null) {
            List<ICollector> collectors = initEmitterRequest(ioDescriptor.getOutputDescriptor());
            workerContext.setCollectors(collectors);
        }
        worker.open(context);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.INIT_CYCLE;
    }

    @Override
    public HighAvailableLevel getHaLevel() {
        return haLevel;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getIterationWindowId() {
        return this.iterationWindowId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public ExecutionTask getTask() {
        return task;
    }

    public void setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    /**
     * Init output emitter.
     */
    protected List<ICollector> initEmitterRequest(OutputDescriptor outputIoDescriptor) {

        InitEmitterRequest request = new InitEmitterRequest(outputIoDescriptor);
        emitterRunner.add(request);
        List<ICollector> collectors = new ArrayList<>(request.getCollectors());
        ((AbstractAlignedWorker) worker).getOutputWriter()
            .setCollectors(collectors);
        return collectors;
    }

    @Override
    public String toString() {
        return "InitCycleEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + '}';
    }
}
