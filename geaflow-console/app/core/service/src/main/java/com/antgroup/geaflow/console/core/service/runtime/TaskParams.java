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

package com.antgroup.geaflow.console.core.service.runtime;

import com.antgroup.geaflow.console.common.util.type.CatalogType;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.ClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.GeaflowArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.HaMetaArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.JobArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.JobConfigClass;
import com.antgroup.geaflow.console.core.model.job.config.MetricArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.PersistentArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.RuntimeMetaArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.StateArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.SystemArgsClass;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.service.config.DeployConfig;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class TaskParams {

    private static final String RUNTIME_TASK_NAME_PREFIX = "geaflow";

    @Autowired
    protected DeployConfig deployConfig;

    public static String getRuntimeTaskName(String taskId) {
        return RUNTIME_TASK_NAME_PREFIX + taskId;
    }

    public void validateRuntimeTaskId(String runtimeTaskId) {
        Preconditions.checkArgument(StringUtils.startsWith(runtimeTaskId, RUNTIME_TASK_NAME_PREFIX),
            "Invalid runtimeTaskId %s", runtimeTaskId);
    }

    protected final GeaflowArgsClass buildGeaflowArgs(GeaflowInstance instance, GeaflowTask task) {
        GeaflowArgsClass geaflowArgs = new GeaflowArgsClass();
        geaflowArgs.setSystemArgs(buildSystemArgs(instance, task));
        geaflowArgs.setClusterArgs(buildClusterArgs(task));
        geaflowArgs.setJobArgs(buildJobArgs(task));
        return geaflowArgs;
    }

    private SystemArgsClass buildSystemArgs(GeaflowInstance instance, GeaflowTask task) {
        SystemArgsClass systemArgs = new SystemArgsClass();

        String taskId = task.getId();
        String runtimeTaskName = getRuntimeTaskName(taskId);
        String runtimeTaskId = runtimeTaskName + "-" + System.currentTimeMillis();

        systemArgs.setTaskId(taskId);
        systemArgs.setRuntimeTaskId(runtimeTaskId);
        systemArgs.setRuntimeTaskName(runtimeTaskName);
        systemArgs.setGateway(deployConfig.getGatewayUrl());
        systemArgs.setTaskToken(task.getToken());
        systemArgs.setStartupNotifyUrl(task.getStartupNotifyUrl(deployConfig.getGatewayUrl()));
        systemArgs.setInstanceName(instance.getName());
        systemArgs.setCatalogType(CatalogType.CONSOLE.getValue());

        StateArgsClass stateArgs = new StateArgsClass();
        stateArgs.setRuntimeMetaArgs(new RuntimeMetaArgsClass(task.getRuntimeMetaPluginConfig()));
        stateArgs.setHaMetaArgs(new HaMetaArgsClass(task.getHaMetaPluginConfig()));
        stateArgs.setPersistentArgs(new PersistentArgsClass(task.getDataPluginConfig()));
        systemArgs.setStateArgs(stateArgs);

        systemArgs.setMetricArgs(new MetricArgsClass(task.getMetricPluginConfig()));
        return systemArgs;
    }

    protected abstract ClusterArgsClass buildClusterArgs(GeaflowTask task);

    private JobArgsClass buildJobArgs(GeaflowTask task) {
        JobArgsClass jobArgs = new JobArgsClass(task.getRelease().getJobConfig().parse(JobConfigClass.class));
        jobArgs.setSystemStateType(GeaflowPluginType.ROCKSDB);
        return jobArgs;
    }
}
