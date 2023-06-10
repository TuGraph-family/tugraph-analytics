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

package com.antgroup.geaflow.cluster.task.runner;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;

import java.io.Serializable;

public interface ITaskRunnerContext extends Serializable {

    /**
     * Returns container id.
     */
    int getContainerId();

    /**
     * Returns index of current task.
     */
    int getWorkerIndex();

    /**
     * Returns the worker config.
     */
    Configuration getConfig();

    /**
     * Returns the metric group ref.
     */
    MetricGroup getMetricGroup();

    /**
     * Returns the fetcher service.
     */
    FetcherService getFetcherService();

    /**
     * Returns teh emitter service.
     */
    EmitterService getEmitterService();
}
