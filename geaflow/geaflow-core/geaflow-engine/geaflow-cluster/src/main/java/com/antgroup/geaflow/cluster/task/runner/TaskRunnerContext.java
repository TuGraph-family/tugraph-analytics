/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.cluster.task.runner;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;

public class TaskRunnerContext implements ITaskRunnerContext {

    private final int workerIndex;
    private final int containerId;
    private final Configuration config;
    private final MetricGroup metricGroup;
    private final FetcherService fetcherService;
    private final EmitterService emitterService;

    public TaskRunnerContext(int containerId, int workerIndex, Configuration config,
                             MetricGroup metricGroup, FetcherService fetcherService,
                             EmitterService emitterService) {
        this.containerId = containerId;
        this.workerIndex = workerIndex;
        this.config = config;
        this.metricGroup = metricGroup;
        this.fetcherService = fetcherService;
        this.emitterService = emitterService;
    }

    @Override
    public int getContainerId() {
        return containerId;
    }

    @Override
    public int getWorkerIndex() {
        return workerIndex;
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public FetcherService getFetcherService() {
        return fetcherService;
    }

    @Override
    public EmitterService getEmitterService() {
        return emitterService;
    }

}
