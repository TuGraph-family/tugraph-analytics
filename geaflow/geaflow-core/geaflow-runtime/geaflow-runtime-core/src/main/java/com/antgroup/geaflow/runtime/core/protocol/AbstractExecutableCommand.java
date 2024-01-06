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

import com.antgroup.geaflow.cluster.collector.EmitterRunner;
import com.antgroup.geaflow.cluster.fetcher.FetcherRunner;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.cluster.worker.IWorker;
import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExecutableCommand implements IExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExecutableCommand.class);

    protected int workerId;

    /**
     * Cycle id of the current event.
     */
    protected int cycleId;

    /**
     * window id of cycle.
     */
    protected long windowId;

    protected transient IWorker worker;
    protected transient IWorkerContext context;
    protected transient FetcherRunner fetcherRunner;
    protected transient EmitterRunner emitterRunner;

    public AbstractExecutableCommand(int workerId, int cycleId, long windowId) {
        this.workerId = workerId;
        this.cycleId = cycleId;
        this.windowId = windowId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        worker = taskContext.getWorker();
        context = worker.getWorkerContext();
        int workerIndex = taskContext.getWorkerIndex();
        fetcherRunner = taskContext.getFetcherService().getRunner(workerIndex);
        emitterRunner = taskContext.getEmitterService().getRunner(workerIndex);
        LOGGER.info("task {} process {} batchId {}",
            context == null ? null : ((AbstractWorkerContext) context).getTaskId(), this, windowId);
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getIterationWindowId() {
        return windowId;
    }

    @Override
    public void interrupt() {
        worker.interrupt();
    }

    /**
     * Finish compute and tell scheduler finish.
     *
     * @param cycleId
     * @param windowId
     * @param eventType
     */
    protected <T> void sendDoneEvent(String driverId, EventType sourceEventType, T result, boolean sendMetrics) {
        AbstractWorkerContext workerContext = (AbstractWorkerContext) this.context;
        int taskId = workerContext.getTaskId();
        EventMetrics eventMetrics = sendMetrics ? workerContext.getEventMetrics() : null;
        DoneEvent<T> doneEvent = new DoneEvent<>(this.cycleId, this.windowId, taskId, sourceEventType, result, eventMetrics);
        RpcClient.getInstance().processPipeline(driverId, doneEvent);
    }

}
