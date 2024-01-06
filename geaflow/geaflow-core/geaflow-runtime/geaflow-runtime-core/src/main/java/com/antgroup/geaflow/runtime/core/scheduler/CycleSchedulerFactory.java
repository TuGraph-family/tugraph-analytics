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

package com.antgroup.geaflow.runtime.core.scheduler;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CycleSchedulerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CycleSchedulerFactory.class);

    public static ICycleScheduler create(IExecutionCycle cycle) {
        ICycleScheduler scheduler;
        switch (cycle.getType()) {
            case GRAPH:
                scheduler = new ExecutionGraphCycleScheduler();
                break;
            case ITERATION:
            case ITERATION_WITH_AGG:
            case PIPELINE:
                scheduler = new PipelineCycleScheduler();
                break;
            default:
                throw new GeaflowRuntimeException(String.format("not support cycle %s yet", cycle));
        }
        LOGGER.info("create scheduler {} for cycle {}", scheduler.getClass().getSimpleName(), cycle.getCycleId());
        return scheduler;
    }
}
