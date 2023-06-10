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

import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.service.TaskService;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowTaskStatusRefresher {

    private static final ExecutorService EXECUTOR_SERVICE;

    static {
        EXECUTOR_SERVICE = new ThreadPoolExecutor(50, 500, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000));
    }

    @Autowired
    private TaskService taskService;

    @Autowired
    private GeaflowTaskOperator taskOperator;

    @Scheduled(cron = "30 * * * * ?")
    void refresh() {
        try {
            ContextHolder.init();
            List<GeaflowId> taskIds = this.getRunningTasks();
            if (CollectionUtils.isEmpty(taskIds)) {
                return;
            }
            log.info("Task status refresh start, task size: {}", taskIds.size());

            for (GeaflowId taskId : taskIds) {
                CompletableFuture.runAsync(() -> {
                    try {
                        // set tenant and user by task
                        ContextHolder.init();
                        ContextHolder.get().setUserId(taskId.getModifierId());
                        ContextHolder.get().setTenantId(taskId.getTenantId());

                        GeaflowTask task = taskService.get(taskId.getId());
                        taskOperator.refreshStatus(task);

                    } catch (Exception e) {
                        log.error("Task {} status refresh error: {}", taskId.getId(), e.getMessage(), e);

                    } finally {
                        ContextHolder.destroy();
                    }

                }, EXECUTOR_SERVICE);
            }

        } finally {
            ContextHolder.destroy();
        }
    }

    private List<GeaflowId> getRunningTasks() {
        return taskService.getTasksByStatus(GeaflowTaskStatus.RUNNING);
    }
}
