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

package com.antgroup.geaflow.kubernetes.operator.core.model.customresource;

import com.antgroup.geaflow.kubernetes.operator.core.model.job.JobState;
import io.fabric8.kubernetes.model.annotation.PrinterColumn;
import lombok.Data;

@Data
public abstract class AbstractJobStatus {

    /**
     * The job id.
     */
    protected Long jobUid;

    /**
     * The state of the GeaFlow job.
     */
    @PrinterColumn(name = "Job State")
    protected JobState state = JobState.INIT;

    /**
     * Last reconciled deployment spec.
     */
    protected String lastReconciledSpec;

    /**
     * Error message of the GeaFlow job.
     */
    protected String errorMessage;

}
