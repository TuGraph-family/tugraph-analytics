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

package com.antgroup.geaflow.console.web.controller.api;

import com.antgroup.geaflow.console.biz.shared.AuthorizationManager;
import com.antgroup.geaflow.console.biz.shared.TaskManager;
import com.antgroup.geaflow.console.biz.shared.view.TaskOperationView;
import com.antgroup.geaflow.console.biz.shared.view.TaskStartupNotifyView;
import com.antgroup.geaflow.console.biz.shared.view.TaskView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.TaskSearch;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetric;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricMeta;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowCycle;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowError;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowOffset;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowPipeline;
import com.antgroup.geaflow.console.core.model.security.GeaflowAuthority;
import com.antgroup.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import com.antgroup.geaflow.console.core.service.security.Resources;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TaskController {

    @Autowired
    private TaskManager taskManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping("/tasks")
    public GeaflowApiResponse<PageList<TaskView>> searchTasks(TaskSearch search) {
        return GeaflowApiResponse.success(taskManager.search(search));
    }

    @GetMapping("/tasks/{taskId}")
    public GeaflowApiResponse<TaskView> getTask(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.get(taskId));
    }

    @PostMapping("/tasks/{taskId}/operations")
    public GeaflowApiResponse<Boolean> operateTask(@PathVariable String taskId,
                                                   @RequestBody TaskOperationView request) {
        authorizationManager.hasAuthority(GeaflowAuthority.EXECUTE, Resources.task(taskId));
        taskManager.operate(taskId, request.getAction());
        return GeaflowApiResponse.success(true);
    }

    @GetMapping("/tasks/{taskId}/status")
    public GeaflowApiResponse<GeaflowTaskStatus> queryTaskStatus(@PathVariable String taskId,
                                                                 @RequestParam(required = false) Boolean refresh) {
        return GeaflowApiResponse.success(taskManager.queryStatus(taskId, refresh));
    }

    @GetMapping("/tasks/{taskId}/pipelines")
    public GeaflowApiResponse<PageList<GeaflowPipeline>> queryTaskPipelines(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.queryPipelines(taskId));
    }

    @GetMapping("/tasks/{taskId}/pipelines/{pipelineName}/cycles")
    public GeaflowApiResponse<PageList<GeaflowCycle>> queryTaskCycles(@PathVariable String taskId,
                                                                      @PathVariable String pipelineName) {
        return GeaflowApiResponse.success(taskManager.queryCycles(taskId, pipelineName));
    }

    @GetMapping("/tasks/{taskId}/errors")
    public GeaflowApiResponse<PageList<GeaflowError>> queryTaskErrors(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.queryErrors(taskId));
    }

    @GetMapping("tasks/{taskId}/metric-meta")
    public GeaflowApiResponse<PageList<GeaflowMetricMeta>> queryTaskMetricMeta(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.queryMetricMeta(taskId));
    }

    @PostMapping("/tasks/{taskId}/metrics")
    public GeaflowApiResponse<PageList<GeaflowMetric>> queryTaskMetrics(@PathVariable String taskId,
                                                                        @RequestBody GeaflowMetricQueryRequest queryRequest) {
        return GeaflowApiResponse.success(taskManager.queryMetrics(taskId, queryRequest));
    }

    @GetMapping("/tasks/{taskId}/offsets")
    public GeaflowApiResponse<PageList<GeaflowOffset>> queryTaskOffsets(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.queryOffsets(taskId));
    }

    @GetMapping("tasks/{taskId}/heartbeat")
    public GeaflowApiResponse<GeaflowHeartbeatInfo> queryTaskHeartbeat(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.queryHeartbeat(taskId));
    }

    @GetMapping("tasks/{taskId}/logs")
    public GeaflowApiResponse<String> getLogs(@PathVariable String taskId) {
        return GeaflowApiResponse.success(taskManager.getLogs(taskId));
    }

    @PostMapping("/tasks/{taskId}/startup-notify")
    public GeaflowApiResponse<Void> startupNotify(@PathVariable String taskId,
                                                  @RequestBody TaskStartupNotifyView startupNotifyView) {
        taskManager.startupNotify(taskId, startupNotifyView);
        return GeaflowApiResponse.success(null);
    }

    @GetMapping("/tasks/{taskId}/files")
    public void downloadTaskFile(HttpServletResponse response, @PathVariable String taskId, @RequestParam String path) {
        taskManager.download(taskId, path, response);
    }
}
