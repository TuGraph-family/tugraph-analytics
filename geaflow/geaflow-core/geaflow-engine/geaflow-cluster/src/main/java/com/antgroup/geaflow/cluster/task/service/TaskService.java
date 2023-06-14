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

package com.antgroup.geaflow.cluster.task.service;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.task.runner.TaskRunner;
import com.antgroup.geaflow.cluster.task.runner.TaskRunnerContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskService extends AbstractTaskService<ICommand, TaskRunner> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskService.class);

    private static final String WORKER_FORMAT = "geaflow-worker-%d";

    private int containerId;
    private int taskNum;
    private Configuration configuration;
    private MetricGroup metricGroup;
    private FetcherService fetcherService;
    private EmitterService emitterService;

    public TaskService(int containerId, int taskNum, Configuration configuration,
                       MetricGroup metricGroup, FetcherService fetcherService, EmitterService emitterService) {
        super(WORKER_FORMAT);
        this.containerId = containerId;
        this.taskNum = taskNum;
        this.configuration = configuration;
        this.metricGroup = metricGroup;
        this.fetcherService = fetcherService;
        this.emitterService = emitterService;
    }

    @Override
    protected TaskRunner[] buildTaskRunner() {
        TaskRunner[] taskRunners = new TaskRunner[taskNum];
        for (int i = 0; i < taskNum; i++) {
            TaskRunner taskRunner = buildTask(containerId, i, configuration,
                metricGroup, fetcherService, emitterService);
            taskRunners[i] = taskRunner;
        }
        return taskRunners;
    }

    private TaskRunner buildTask(int containerId, int taskIndex,
                                        Configuration configuration, MetricGroup metricGroup,
                                        FetcherService fetcherService, EmitterService emitterService) {
        TaskRunner taskRunner = new TaskRunner();
        TaskRunnerContext taskRunnerContext = new TaskRunnerContext(containerId, taskIndex,
            configuration, metricGroup, fetcherService, emitterService);
        taskRunner.init(taskRunnerContext);
        return taskRunner;
    }
}
