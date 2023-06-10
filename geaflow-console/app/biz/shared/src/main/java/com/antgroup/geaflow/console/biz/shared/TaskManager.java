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

package com.antgroup.geaflow.console.biz.shared;


import com.antgroup.geaflow.console.biz.shared.view.TaskStartupNotifyView;
import com.antgroup.geaflow.console.biz.shared.view.TaskView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.TaskSearch;
import com.antgroup.geaflow.console.common.util.type.GeaflowOperationType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetric;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricMeta;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowCycle;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowError;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowOffset;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowPipeline;
import com.antgroup.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import javax.servlet.http.HttpServletResponse;

public interface TaskManager extends IdManager<TaskView, TaskSearch> {

    TaskView getByJobId(String jobId);

    void operate(String taskId, GeaflowOperationType action);

    GeaflowTaskStatus queryStatus(String taskId, Boolean refresh);

    PageList<GeaflowPipeline> queryPipelines(String taskId);

    PageList<GeaflowCycle> queryCycles(String taskId, String pipelineId);

    PageList<GeaflowError> queryErrors(String taskId);

    PageList<GeaflowMetricMeta> queryMetricMeta(String taskId);

    PageList<GeaflowMetric> queryMetrics(String taskId, GeaflowMetricQueryRequest queryRequest);

    PageList<GeaflowOffset> queryOffsets(String taskId);

    GeaflowHeartbeatInfo queryHeartbeat(String taskId);

    void startupNotify(String taskId, TaskStartupNotifyView requestData);

    void download(String taskId, String path, HttpServletResponse response);

    String getLogs(String taskId);
}
