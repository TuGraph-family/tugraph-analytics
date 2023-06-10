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

package com.antgroup.geaflow.runtime.core.scheduler.cycle;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;

public interface IExecutionCycle {

    /**
     * Returns pipeline name.
     */
    String getPipelineName();

    /**
     * Returns cycle id.
     */
    int getCycleId();

    /**
     * Returns flying count.
     */
    int getFlyingCount();

    /**
     * Returns iteration count.
     */
    long getIterationCount();

    /**
     * Returns config.
     */
    Configuration getConfig();

    /**
     * Returns execution cycle type.
     */
    ExecutionCycleType getType();

    /**
     * Returns driver id.
     */
    String getDriverId();

    /**
     * Returns HA level.
     */
    HighAvailableLevel getHighAvailableLevel();

}
