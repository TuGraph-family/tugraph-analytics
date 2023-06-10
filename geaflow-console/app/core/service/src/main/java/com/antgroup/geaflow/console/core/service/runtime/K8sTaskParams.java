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

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.ClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.ClusterConfigClass;
import com.antgroup.geaflow.console.core.model.job.config.K8SClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.K8sClientArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.K8sClientStopArgsClass;
import com.antgroup.geaflow.console.core.model.plugin.config.K8sPluginConfigClass;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.task.GeaflowTaskHandle;
import com.google.common.base.Preconditions;
import org.springframework.stereotype.Component;

@Component
public class K8sTaskParams extends TaskParams {

    public K8sClientArgsClass buildClientArgs(GeaflowInstance instance, GeaflowTask task) {
        return new K8sClientArgsClass(buildGeaflowArgs(instance, task), task.getMainClass());
    }

    public K8sClientStopArgsClass buildClientStopArgs(GeaflowTask task) {
        GeaflowTaskHandle handle = task.getHandle();
        Preconditions.checkNotNull(handle, "Task %s handle can't be empty", task.getId());

        return new K8sClientStopArgsClass(handle.getAppId(), ((K8SClusterArgsClass) buildClusterArgs(task)));
    }

    @Override
    protected ClusterArgsClass buildClusterArgs(GeaflowTask task) {
        GeaflowRelease release = task.getRelease();

        K8SClusterArgsClass clusterArgs = new K8SClusterArgsClass();
        ClusterConfigClass clusterConfig = release.getClusterConfig().parse(ClusterConfigClass.class);
        K8sPluginConfigClass k8sPluginConfig = release.getCluster().getConfig().parse(K8sPluginConfigClass.class);

        clusterArgs.setTaskClusterConfig(clusterConfig);
        clusterArgs.setClusterConfig(k8sPluginConfig);
        clusterArgs.setEngineJarUrls(JSON.toJSONString(task.getVersionFiles(deployConfig.getGatewayUrl())));
        clusterArgs.setTaskJarUrls(JSON.toJSONString(task.getUserFiles(deployConfig.getGatewayUrl())));
        return clusterArgs;
    }

}
