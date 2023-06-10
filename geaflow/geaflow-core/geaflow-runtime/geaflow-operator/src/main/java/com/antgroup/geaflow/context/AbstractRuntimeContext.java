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

package com.antgroup.geaflow.context;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;

public abstract class AbstractRuntimeContext implements RuntimeContext {

    protected String workPath;
    protected Configuration jobConfig;
    protected MetricGroup metricGroup;
    protected long windowId;

    public AbstractRuntimeContext(Configuration jobConfig) {
        this.jobConfig = jobConfig;
        this.workPath = jobConfig.getString(JOB_WORK_PATH);
    }

    public AbstractRuntimeContext(Configuration jobConfig, MetricGroup metricGroup,
                                  String workPath) {
        this.jobConfig = jobConfig;
        this.metricGroup = metricGroup;
        this.workPath = workPath;
    }

    @Override
    public String getWorkPath() {
        return this.workPath;
    }

    @Override
    public MetricGroup getMetric() {
        return this.metricGroup;
    }

    public void updateWindowId(long windowId) {
        this.windowId = windowId;
    }
}
