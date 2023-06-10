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

import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.ClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.ClusterConfigClass;
import com.antgroup.geaflow.console.core.model.job.config.ContainerClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.GeaflowArgsClass;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import org.springframework.stereotype.Component;

@Component
public class ContainerTaskParams extends TaskParams {

    public GeaflowArgsClass buildClientArgs(GeaflowInstance instance, GeaflowTask task) {
        return buildGeaflowArgs(instance, task);
    }

    @Override
    protected ClusterArgsClass buildClusterArgs(GeaflowTask task) {
        GeaflowRelease release = task.getRelease();

        ContainerClusterArgsClass clusterArgs = new ContainerClusterArgsClass();
        ClusterConfigClass clusterConfig = release.getClusterConfig().parse(ClusterConfigClass.class);
        clusterArgs.setTaskClusterConfig(clusterConfig);
        return clusterArgs;
    }

}
