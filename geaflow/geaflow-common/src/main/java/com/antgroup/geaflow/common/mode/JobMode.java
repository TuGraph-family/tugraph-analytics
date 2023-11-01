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

package com.antgroup.geaflow.common.mode;


import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;

public enum JobMode {
    /**
     * compute job mode.
     */
    COMPUTE,

    /**
     * Olap service job mode.
     */
    OLAP_SERVICE,

    /**
     * State service job mode.
     */
    STATE_SERVICE;

    public static JobMode getJobMode(Configuration configuration) {
        String jobMode = configuration.getString(ExecutionConfigKeys.JOB_MODE);
        return valueOf(jobMode.toUpperCase());
    }
}
