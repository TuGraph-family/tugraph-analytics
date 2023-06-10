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
import com.antgroup.geaflow.console.common.service.integration.engine.K8sJobClient;
import com.antgroup.geaflow.console.common.util.ThreadUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowLogException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.GeaflowArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.K8SClusterArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.K8sClientArgsClass;
import com.antgroup.geaflow.console.core.model.job.config.K8sClientStopArgsClass;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.task.GeaflowTaskHandle;
import com.antgroup.geaflow.console.core.model.task.K8sTaskHandle;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class K8sRuntime implements GeaflowRuntime {

    @Autowired
    private K8sTaskParams taskParams;

    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private InstanceService instanceService;

    @Override
    public GeaflowTaskHandle start(GeaflowTask task) {
        GeaflowInstance instance = instanceService.get(task.getRelease().getJob().getInstanceId());
        return doStart(task, taskParams.buildClientArgs(instance, task));
    }

    @Override
    public void stop(GeaflowTask task) {
        doStop(task, taskParams.buildClientStopArgs(task));
    }

    @Override
    public GeaflowTaskStatus queryStatus(GeaflowTask task) {
        try {
            return queryStatusWithRetry(task, taskParams.buildClientStopArgs(task), 5);

        } catch (Exception e) {
            log.error("Query task {} status failed, handle={}", task.getId(), JSON.toJSONString(task.getHandle()), e);
            return GeaflowTaskStatus.FAILED;
        }
    }

    private GeaflowTaskHandle doStart(GeaflowTask task, K8sClientArgsClass clientArgs) {
        GeaflowArgsClass geaflowArgs = clientArgs.getGeaflowArgs();
        K8SClusterArgsClass clusterArgs = (K8SClusterArgsClass) geaflowArgs.getClusterArgs();
        String runtimeTaskId = geaflowArgs.getSystemArgs().getRuntimeTaskId();
        taskParams.validateRuntimeTaskId(runtimeTaskId);

        try {
            Map<String, String> params = clientArgs.build().toStringMap();
            String masterUrl = clusterArgs.getClusterConfig().getMasterUrl();

            VersionClassLoader loader = versionFactory.getClassLoader(task.getRelease().getVersion());
            K8sJobClient jobClient = loader.newInstance(K8sJobClient.class, params, masterUrl);
            jobClient.submitJob();

            K8sTaskHandle taskHandle = new K8sTaskHandle();
            taskHandle.setAppId(runtimeTaskId);
            taskHandle.setClusterType(GeaflowPluginType.K8S);

            log.info("Start task {} success, handle={}", task.getId(), JSON.toJSONString(taskHandle));
            return taskHandle;

        } catch (Exception e) {
            throw new GeaflowLogException("Start task {} failed", task.getId(), e);
        }
    }

    private void doStop(GeaflowTask task, K8sClientStopArgsClass k8sClientStopArgs) {
        taskParams.validateRuntimeTaskId(k8sClientStopArgs.getRuntimeTaskId());

        try {
            Map<String, String> params = k8sClientStopArgs.build().toStringMap();
            String masterUrl = k8sClientStopArgs.getClusterArgs().getClusterConfig().getMasterUrl();

            VersionClassLoader loader = versionFactory.getClassLoader(task.getRelease().getVersion());
            K8sJobClient jobClient = loader.newInstance(K8sJobClient.class, params, masterUrl);

            jobClient.stopJob();
            log.info("Stop task {} success, handle={}", task.getId(), JSON.toJSONString(task.getHandle()));

        } catch (Exception e) {
            throw new GeaflowLogException("Stop task {} failed", task.getId(), e);
        }
    }

    private GeaflowTaskStatus queryStatusWithRetry(GeaflowTask task, K8sClientStopArgsClass k8sClientStopArgs,
                                                   int retryTimes) {
        while (retryTimes > 0) {
            try {
                boolean existMasterService = existMasterService(task, k8sClientStopArgs);
                return existMasterService ? GeaflowTaskStatus.RUNNING : GeaflowTaskStatus.FAILED;

            } catch (Exception e) {
                if (--retryTimes == 0) {
                    throw e;
                }

                ThreadUtil.sleepMilliSeconds(500);
            }
        }

        return task.getStatus();
    }

    private boolean existMasterService(GeaflowTask task, K8sClientStopArgsClass k8sClientStopArgs) {
        Map<String, String> params = k8sClientStopArgs.build().toStringMap();
        String masterUrl = k8sClientStopArgs.getClusterArgs().getClusterConfig().getMasterUrl();

        VersionClassLoader loader = versionFactory.getClassLoader(task.getRelease().getVersion());
        K8sJobClient jobClient = loader.newInstance(K8sJobClient.class, params, masterUrl);

        return jobClient.getMasterService() != null;
    }
}
