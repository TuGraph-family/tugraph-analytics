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

package com.antgroup.geaflow.console.core.service.store;

import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.core.model.metric.GeaflowMetricMeta;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowCycle;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowError;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowOffset;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowPipeline;
import com.antgroup.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;

public interface GeaflowRuntimeMetaStore {

    PageList<GeaflowPipeline> queryPipelines(GeaflowTask task);

    PageList<GeaflowCycle> queryCycles(GeaflowTask task, String pipelineId);

    PageList<GeaflowOffset> queryOffsets(GeaflowTask task);

    PageList<GeaflowError> queryErrors(GeaflowTask task);

    PageList<GeaflowMetricMeta> queryMetricMeta(GeaflowTask task);

    GeaflowHeartbeatInfo queryHeartbeat(GeaflowTask task);

    void cleanRuntimeMeta(GeaflowTask task);

}
