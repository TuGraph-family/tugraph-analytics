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

package com.antgroup.geaflow.console.common.dal.entity;

import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskType;
import com.baomidou.mybatisplus.annotation.TableName;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("geaflow_task")
public class TaskEntity extends IdEntity {

    private String jobId;

    private String releaseId;

    private String token;

    private Date startTime;

    private Date endTime;

    private GeaflowTaskType type;

    private GeaflowTaskStatus status;

    private String runtimeMetaConfigId;

    private String haMetaConfigId;

    private String metricConfigId;

    private String dataConfigId;

    private String host;

    private String handle;
}
