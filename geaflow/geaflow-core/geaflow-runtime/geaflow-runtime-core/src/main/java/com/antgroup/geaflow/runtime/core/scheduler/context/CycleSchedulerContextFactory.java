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

package com.antgroup.geaflow.runtime.core.scheduler.context;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CycleSchedulerContextFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CycleSchedulerContextFactory.class);

    public static ICycleSchedulerContext build(Supplier<? extends ICycleSchedulerContext> func) {
        return CheckpointSchedulerContext.build(func);
    }

    public static ICycleSchedulerContext create(IExecutionCycle cycle, ICycleSchedulerContext parent) {
        AbstractCycleSchedulerContext context;
        switch (cycle.getHighAvailableLevel()) {
            case CHECKPOINT:
                LOGGER.info("create checkpoint scheduler context");
                context = new CheckpointSchedulerContext(cycle, parent);
                break;
            case REDO:
                if (cycle.getType() == ExecutionCycleType.ITERATION
                    || cycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG) {
                    LOGGER.info("create iteration redo scheduler context");
                    context = new IterationRedoSchedulerContext(cycle, parent);
                } else {
                    LOGGER.info("create redo scheduler context");
                    context = new RedoSchedulerContext(cycle, parent);
                }
                break;
            default:
                throw new GeaflowRuntimeException(String.format("not support ha level %s for cycle %s",
                    cycle.getHighAvailableLevel(), cycle.getCycleId()));
        }
        context.init();
        return context;
    }
}
