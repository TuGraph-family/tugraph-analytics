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

package com.antgroup.geaflow.console.core.service.task;

import static com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus.FAILED;
import static com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus.RUNNING;
import static com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus.STARTING;

import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.util.DateTimeUtil;
import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.type.GeaflowOperationType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetric;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricMeta;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowAudit;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowCycle;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowError;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowOffset;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowPipeline;
import com.antgroup.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import com.antgroup.geaflow.console.core.model.task.GeaflowHeartbeatInfo.ContainerInfo;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.task.GeaflowTaskHandle;
import com.antgroup.geaflow.console.core.service.AuditService;
import com.antgroup.geaflow.console.core.service.TaskService;
import com.antgroup.geaflow.console.core.service.runtime.GeaflowRuntime;
import com.antgroup.geaflow.console.core.service.runtime.RuntimeFactory;
import com.antgroup.geaflow.console.core.service.security.TokenGenerator;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.store.GeaflowHaMetaStore;
import com.antgroup.geaflow.console.core.service.store.GeaflowMetricStore;
import com.antgroup.geaflow.console.core.service.store.GeaflowRuntimeMetaStore;
import com.antgroup.geaflow.console.core.service.store.factory.DataStoreFactory;
import com.antgroup.geaflow.console.core.service.store.factory.HaMetaStoreFactory;
import com.antgroup.geaflow.console.core.service.store.factory.MetricStoreFactory;
import com.antgroup.geaflow.console.core.service.store.factory.RuntimeMetaStoreFactory;
import java.util.Date;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowTaskOperator {

    private static final int MASTER_API_PORT = 8090;

    private static final String COLON = ":";

    private static final int TASK_STARTUP_TIMEOUT = 10 * 60;

    @Autowired
    private TaskService taskService;

    @Autowired
    private TokenGenerator tokenGenerator;

    @Autowired
    private RuntimeFactory runtimeFactory;

    @Autowired
    private RuntimeMetaStoreFactory runtimeMetaStoreFactory;

    @Autowired
    private MetricStoreFactory metricStoreFactory;

    @Autowired
    private HaMetaStoreFactory haMetaStoreFactory;

    @Autowired
    private DataStoreFactory dataStoreFactory;

    @Autowired
    private AuditService auditService;

    public boolean start(GeaflowTask task) {
        GeaflowRuntime runtime = runtimeFactory.getRuntime(task);

        // generate task token
        task.setToken(tokenGenerator.nextTaskToken());

        // submit job to the engine
        try {
            GeaflowTaskHandle handle = runtime.start(task);
            task.setHandle(handle);
            task.setStartTime(new Date());
            taskService.update(task);
            taskService.updateStatus(task.getId(), STARTING, RUNNING);
            log.info("Submit task {} successfully", task.getId());
            return true;

        } catch (Exception e) {
            log.error("Submit task {} failed", task.getId(), e);
            taskService.updateStatus(task.getId(), STARTING, FAILED);
            throw e;
        }
    }

    public void stop(GeaflowTask task) {
        runtimeFactory.getRuntime(task).stop(task);
    }

    public GeaflowTaskStatus refreshStatus(GeaflowTask task) {
        GeaflowTaskStatus oldStatus = task.getStatus();

        // only refresh running task
        if (!RUNNING.equals(oldStatus)) {
            return oldStatus;
        }

        GeaflowTaskStatus newStatus = runtimeFactory.getRuntime(task).queryStatus(task);
        if (newStatus == FAILED) {
            // task has not been started completely
            if (!Optional.ofNullable(task.getHandle()).map(GeaflowTaskHandle::getStartupNotifyInfo).isPresent()) {
                if (DateTimeUtil.isExpired(task.getStartTime(), TASK_STARTUP_TIMEOUT)) {
                    // release task resource
                    this.stop(task);

                    // waiting startup timeout
                    String detail = Fmt.as("Waiting task startup timeout after {}s", TASK_STARTUP_TIMEOUT);
                    log.info(detail);
                    auditService.create(new GeaflowAudit(task.getId(), GeaflowOperationType.STOP, detail));

                } else {
                    // waiting startup, keep status not changed
                    newStatus = oldStatus;
                }
            }
        }

        taskService.updateStatus(task.getId(), oldStatus, newStatus);
        return newStatus;
    }

    public boolean cleanMeta(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowPluginConfig haMetaConfig = task.getHaMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        GeaflowHaMetaStore haMetaStore = haMetaStoreFactory.getHaMetaStore(haMetaConfig);
        runtimeMetaStore.cleanRuntimeMeta(task);
        haMetaStore.cleanHaMeta(task);
        return true;
    }

    public boolean cleanData(GeaflowTask task) {
        GeaflowPluginConfig dataConfig = task.getDataPluginConfig();
        GeaflowDataStore dataStore = dataStoreFactory.getDataStore(dataConfig);
        dataStore.cleanData(task);
        return true;
    }

    public PageList<GeaflowPipeline> queryPipelines(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryPipelines(task);
    }

    public PageList<GeaflowCycle> queryCycles(GeaflowTask task, String pipelineId) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryCycles(task, pipelineId);
    }

    public PageList<GeaflowMetricMeta> queryMetricMeta(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryMetricMeta(task);
    }

    public PageList<GeaflowMetric> queryMetrics(GeaflowTask task, GeaflowMetricQueryRequest queryRequest) {
        GeaflowPluginConfig metricConfig = task.getMetricPluginConfig();
        GeaflowMetricStore metaStore = metricStoreFactory.getMetricStore(metricConfig);
        return metaStore.queryMetrics(task, queryRequest);
    }

    public PageList<GeaflowOffset> queryOffsets(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryOffsets(task);
    }

    public PageList<GeaflowError> queryErrors(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryErrors(task);
    }

    public GeaflowHeartbeatInfo queryHeartbeat(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        GeaflowHeartbeatInfo heartbeatInfo = runtimeMetaStore.queryHeartbeat(task);
        setupHeartbeatInfo(heartbeatInfo);
        return heartbeatInfo;
    }

    private void setupHeartbeatInfo(GeaflowHeartbeatInfo heartbeatInfo) {
        if (heartbeatInfo != null) {
            int activeNum = 0;
            long now = System.currentTimeMillis();
            long expiredTime = now - heartbeatInfo.getExpiredTimeMs();
            for (ContainerInfo container : heartbeatInfo.getContainers()) {
                if (container.getLastTimestamp() != null && container.getLastTimestamp() > expiredTime) {
                    container.setActive(true);
                    activeNum ++;
                }
            }
            heartbeatInfo.setActiveNum(activeNum);
        }
    }
}
