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

package com.antgroup.geaflow.console.core.model.statement;

import com.antgroup.geaflow.console.common.util.type.GeaflowStatementStatus;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class GeaflowStatement extends GeaflowId {

    private String script;

    private GeaflowStatementStatus status;

    private String result;

    private String jobId;

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(jobId, "JobId is null");
        Preconditions.checkNotNull(script, "Query script is null");
    }
}
