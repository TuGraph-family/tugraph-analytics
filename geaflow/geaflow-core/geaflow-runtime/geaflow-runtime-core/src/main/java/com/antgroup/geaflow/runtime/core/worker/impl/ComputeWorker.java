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

package com.antgroup.geaflow.runtime.core.worker.impl;

import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.cluster.worker.IAffinityWorker;
import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.common.utils.GcUtil;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContextManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeWorker<R> extends AbstractAlignedWorker<R> implements TransactionTrait, IAffinityWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeWorker.class);
    private boolean isTransactionProcessor;

    public ComputeWorker() {
        super();
    }

    @Override
    public void open(IWorkerContext workerContext) {
        super.open(workerContext);
        LOGGER.info("open processor");
        context.getProcessor().open(
            outputWriter.getCollectors(),
            context.getRuntimeContext()
        );
        this.isTransactionProcessor = context.getProcessor() instanceof TransactionTrait;
    }

    @Override
    public void init(long windowId) {
        LOGGER.info("taskId {} init windowId {}", context.getTaskId(), windowId);
        updateWindowId(windowId);
        context.getProcessor().init(windowId);
    }

    @Override
    public R process(BatchRecord batchRecord) {
        return (R) context.getProcessor().process(batchRecord);
    }

    @Override
    public void finish(long windowId) {
        LOGGER.info("taskId {} finishes windowId {}, currentBatchId {}",
            context.getTaskId(), windowId, context.getCurrentWindowId());
        long startFinishTime = System.currentTimeMillis();
        context.getProcessor().finish(windowId);

        EventMetrics eventMetrics = context.getEventMetrics();
        eventMetrics.setFinishTime(System.currentTimeMillis() - startFinishTime);
        eventMetrics.setExecuteTime(System.currentTimeMillis() - eventMetrics.getStartTs());
        eventMetrics.setGcTime(GcUtil.computeCurrentTotalGcTime() - eventMetrics.getStartGcTs());
        finishWindow(context.getCurrentWindowId());
    }

    @Override
    public void rollback(long windowId) {
        LOGGER.info("taskId {} rollback windowId {}", context.getTaskId(), windowId);
        if (isTransactionProcessor) {
            ((TransactionTrait) context.getProcessor()).rollback(windowId);
        }
        updateWindowId(windowId + 1);
    }

    @Override
    public void stash() {
        // Stash current worker context.
        WorkerContextManager.register(context.getTaskId(), (WorkerContext) context);
        context = null;
    }

    @Override
    public void pop(IWorkerContext workerContext) {
        AbstractWorkerContext popWorkerContext = (AbstractWorkerContext) workerContext;
        context = (AbstractWorkerContext) WorkerContextManager.get(popWorkerContext.getTaskId());
        Preconditions.checkArgument(context != null, "not found any context");

        context.setPipelineId(popWorkerContext.getPipelineId());
        context.setPipelineName(popWorkerContext.getPipelineName());
        context.setWindowId(popWorkerContext.getWindowId());

        // Update runtime context.
        DefaultRuntimeContext runtimeContext = (DefaultRuntimeContext) context.getRuntimeContext();
        runtimeContext.setPipelineId(context.getPipelineId());
        runtimeContext.setPipelineName(context.getPipelineName());
        runtimeContext.setWindowId(context.getWindowId());

        // Update collectors.
        for (ICollector collector : context.getCollectors()) {
            LOGGER.info("setup collector {}", runtimeContext.getTaskArgs());
            collector.setUp(runtimeContext);
        }
        getOutputWriter().setCollectors(this.context.getCollectors());
    }
}
