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

package com.antgroup.geaflow.api.context;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;

import java.io.Serializable;
import java.util.Map;

public interface RuntimeContext extends Serializable, Cloneable {

    /**
     * Returns pipeline id.
     */
    long getPipelineId();

    /**
     * Returns pipeline name.
     */
    String getPipelineName();

    /**
     * Get Relevant information of task.
     */
    TaskArgs getTaskArgs();

    /**
     * Returns runtime configuration.
     */
    Configuration getConfiguration();

    /**
     * Returns runtime work path dir.
     */
    String getWorkPath();

    /**
     * Returns metric group ref.
     */
    MetricGroup getMetric();

    /**
     * Clone runtime context and put all opConfig into runtime config.
     * @param opConfig The config of corresponding operator.
     */
    RuntimeContext clone(Map<String, String> opConfig);

    /**
     * Returns current window id.
     */
    long getWindowId();
}
