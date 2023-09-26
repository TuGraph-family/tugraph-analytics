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

package com.antgroup.geaflow.runtime.core.context;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.context.AbstractRuntimeContext;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import java.util.HashMap;
import java.util.Map;

public class DefaultRuntimeContext extends AbstractRuntimeContext {

    private long pipelineId;
    private String pipelineName;
    private TaskArgs taskArgs;
    private long windowId;
    protected IoDescriptor ioDescriptor;

    public DefaultRuntimeContext(Configuration jobConfig) {
        super(jobConfig);
    }

    @Override
    public long getPipelineId() {
        return pipelineId;
    }

    public DefaultRuntimeContext setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
        return this;
    }

    @Override
    public String getPipelineName() {
        return pipelineName;
    }

    public DefaultRuntimeContext setPipelineName(String jobName) {
        this.pipelineName = jobName;
        return this;
    }

    @Override
    public TaskArgs getTaskArgs() {
        return taskArgs;
    }

    public DefaultRuntimeContext setTaskArgs(TaskArgs taskArgs) {
        this.taskArgs = taskArgs;
        return this;
    }

    @Override
    public Configuration getConfiguration() {
        return this.jobConfig;
    }

    public DefaultRuntimeContext setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
        return this;
    }

    public DefaultRuntimeContext setWorkPath(String workPath) {
        this.workPath = workPath;
        return this;
    }

    public DefaultRuntimeContext setMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public DefaultRuntimeContext setWindowId(long windowId) {
        this.windowId = windowId;
        return this;
    }

    @Override
    public long getWindowId() {
        return windowId;
    }

    @Override
    public RuntimeContext clone(Map<String, String> opConfig) {
        Map<String, String> newConfig = new HashMap<>();
        newConfig.putAll(jobConfig.getConfigMap());
        newConfig.putAll(opConfig);
        Configuration configuration = new Configuration(newConfig);
        return DefaultRuntimeContext.build(configuration)
            .setTaskArgs(taskArgs)
            .setPipelineId(pipelineId)
            .setPipelineName(pipelineName)
            .setMetricGroup(metricGroup)
            .setIoDescriptor(ioDescriptor)
            .setWorkPath(getWorkPath())
            .setWindowId(windowId);
    }

    public static DefaultRuntimeContext build(Configuration configuration) {
        DefaultRuntimeContext runtimeContext = new DefaultRuntimeContext(configuration);
        return runtimeContext;
    }
}
