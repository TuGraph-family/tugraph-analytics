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

package com.antgroup.geaflow.console.core.model.runtime;

import com.antgroup.geaflow.console.common.util.type.GeaflowOperationType;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowAudit extends GeaflowId {

    private GeaflowOperationType operationType;

    private String resourceId;

    private GeaflowResourceType resourceType;

    private String detail;

    public GeaflowAudit(GeaflowTask task, GeaflowOperationType operationType,
                        String detail) {
        this.operationType = operationType;
        this.resourceId = task.getId();
        this.resourceType = GeaflowResourceType.TASK;
        this.detail = detail;
    }

    public GeaflowAudit(GeaflowTask task, GeaflowOperationType operationType) {
        this(task, operationType, null);
    }
}
