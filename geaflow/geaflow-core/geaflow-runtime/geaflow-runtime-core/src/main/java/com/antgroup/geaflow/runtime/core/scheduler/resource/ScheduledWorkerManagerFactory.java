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
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;

public class ScheduledWorkerManagerFactory {

    public static IScheduledWorkerManager createScheduledWorkerManager(Configuration config,
                                                                       HighAvailableLevel level) {
        switch (level) {
            case REDO:
                return AbstractScheduledWorkerManager.getInstance(config, RedoCycleScheduledWorkerManager.class);
            case CHECKPOINT:
                return AbstractScheduledWorkerManager.getInstance(config, CheckpointCycleScheduledWorkerManager.class);
            default:
                throw new GeaflowRuntimeException("not support worker manager type " + level);
        }

    }

    public static HighAvailableLevel getWorkerManagerHALevel(IExecutionCycle cycle) {
        if (cycle.getType() == ExecutionCycleType.GRAPH) {
            ExecutionGraphCycle graph = (ExecutionGraphCycle) cycle;
            if (graph.getHighAvailableLevel() == HighAvailableLevel.CHECKPOINT) {
                return HighAvailableLevel.CHECKPOINT;
            }
            // As for stream case, the whole graph is REDO ha level while child cycle is CHECKPOINT.
            // We need set worker manager ha level to CHECKPOINT
            // to make sure all request worker initialized with CHECKPOINT level.
            if (graph.getCycleMap().size() == 1) {
                IExecutionCycle child = graph.getCycleMap().values().iterator().next();
                if (child.getHighAvailableLevel() == HighAvailableLevel.CHECKPOINT) {
                    return HighAvailableLevel.CHECKPOINT;
                }
            }
            return HighAvailableLevel.REDO;

        } else {
            return cycle.getHighAvailableLevel();
        }
    }
}
