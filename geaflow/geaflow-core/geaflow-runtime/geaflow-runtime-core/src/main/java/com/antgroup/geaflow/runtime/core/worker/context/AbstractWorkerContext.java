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

package com.antgroup.geaflow.runtime.core.worker.context;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.cluster.protocol.IEventContext;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.processor.Processor;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import com.antgroup.geaflow.runtime.core.context.EventContext;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import java.util.List;

public abstract class AbstractWorkerContext implements IWorkerContext {

    protected long currentWindowId;
    protected int taskId;
    protected Processor processor;
    protected Configuration config;
    protected boolean enableDebug;

    protected ExecutionTask executionTask;
    protected IoDescriptor ioDescriptor;
    protected int cycleId;
    protected long pipelineId;
    protected long schedulerId;
    protected String pipelineName;
    protected String driverId;
    protected MetricGroup metricGroup;
    protected EventMetrics eventMetrics;
    protected List<ICollector<?>> collectors;
    protected long windowId;
    protected RuntimeContext runtimeContext;
    protected boolean isFinished;

    public AbstractWorkerContext(ITaskContext taskContext) {
        this.config = taskContext.getConfig();
        this.metricGroup = taskContext.getMetricGroup();
        this.enableDebug = false;
    }

    @Override
    public void init(IEventContext eventContext) {
        EventContext context = (EventContext) eventContext;
        this.currentWindowId = context.getCurrentWindowId();
        this.cycleId = context.getCycleId();
        this.pipelineId = context.getPipelineId();
        this.schedulerId = context.getSchedulerId();
        this.pipelineName = context.getPipelineName();
        this.driverId = context.getDriverId();
        this.ioDescriptor = context.getIoDescriptor();
        this.executionTask = context.getExecutionTask();
        this.processor = executionTask.getProcessor();
        this.taskId = executionTask.getTaskId();
        this.windowId = context.getWindowId();
        this.runtimeContext = createRuntimeContext();
        this.isFinished = false;
        this.initEventMetrics();
    }

    /**
     * Create runtime context and set io descriptor.
     */
    private RuntimeContext createRuntimeContext() {
        return DefaultRuntimeContext.build(config)
            .setTaskArgs(this.executionTask.buildTaskArgs())
            .setPipelineId(pipelineId)
            .setPipelineName(pipelineName)
            .setMetricGroup(metricGroup)
            .setIoDescriptor(ioDescriptor)
            .setWindowId(windowId);
    }

    public long getCurrentWindowId() {
        return currentWindowId;
    }

    public void setCurrentWindowId(long currentWindowId) {
        this.currentWindowId = currentWindowId;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getCycleId() {
        return cycleId;
    }

    public Processor getProcessor() {
        return processor;
    }

    public ExecutionTask getExecutionTask() {
        return executionTask;
    }

    public EventMetrics getEventMetrics() {
        return eventMetrics;
    }

    public List<ICollector<?>> getCollectors() {
        return collectors;
    }

    public void setCollectors(List<ICollector<?>> collectors) {
        this.collectors = collectors;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public void setSchedulerId(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }

    public long getSchedulerId() {
        return schedulerId;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public void setWindowId(long windowId) {
        this.windowId = windowId;
    }

    public long getWindowId() {
        return windowId;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public boolean isIterativeTask() {
        return this.executionTask.isIterative();
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public void initEventMetrics() {
        this.eventMetrics = new EventMetrics(
            this.executionTask.getVertexId(),
            this.executionTask.getParallelism(),
            this.executionTask.getIndex());
    }

}
