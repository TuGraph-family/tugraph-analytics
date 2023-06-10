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

package com.antgroup.geaflow.cluster.task;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.cluster.task.runner.ITaskRunnerContext;
import com.antgroup.geaflow.cluster.worker.IWorker;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;

public class TaskContext implements ITaskContext {

    private int workerIndex;
    private Configuration config;
    private MetricGroup metricGroup;

    private IWorker worker;
    private FetcherService fetcherService;
    private EmitterService emitterService;

    public TaskContext(ITaskRunnerContext taskContext) {
        this.workerIndex = taskContext.getWorkerIndex();
        this.config = taskContext.getConfig();
        this.metricGroup = taskContext.getMetricGroup();
        this.fetcherService = taskContext.getFetcherService();
        this.emitterService = taskContext.getEmitterService();
    }

    @Override
    public IWorker getWorker() {
        return worker;
    }

    @Override
    public void registerWorker(IWorker worker) {
        this.worker = worker;
    }

    @Override
    public FetcherService getFetcherService() {
        return fetcherService;
    }

    @Override
    public EmitterService getEmitterService() {
        return emitterService;
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

    /**
     * Close worker and io resources.
     */
    @Override
    public void close() {
        worker.close();
        fetcherService.shutdown();
        emitterService.shutdown();
    }

}
