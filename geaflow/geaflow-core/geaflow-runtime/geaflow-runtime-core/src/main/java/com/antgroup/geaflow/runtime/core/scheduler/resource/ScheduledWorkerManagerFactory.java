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

package com.antgroup.geaflow.runtime.core.scheduler.resource;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;

public class ScheduledWorkerManagerFactory {

    public static IScheduledWorkerManager createScheduledWorkerManager(Configuration config,
                                                                       HighAvailableLevel level) {
        switch (level) {
            case REDO:
                return new RedoCycleScheduledWorkerManager(config);
            case CHECKPOINT:
                return new CheckpointCycleScheduledWorkerManager(config);
            default:
                throw new GeaflowRuntimeException("not support worker manager type " + level);

        }
    }
}
