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

import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IterationRedoSchedulerContext<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>> extends RedoSchedulerContext<C, PC, PCC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IterationRedoSchedulerContext.class);

    public IterationRedoSchedulerContext(C cycle, PCC parentContext) {
        super(cycle, parentContext);
    }

    public void init(long startIterationId) {
        super.init(DEFAULT_INITIAL_ITERATION_ID);
        this.initialIterationId = parentContext.getCurrentIterationId();
        this.finishIterationId = cycle.getIterationCount();
        LOGGER.info("init cycle for iteration, initialIterationId {}, finishIterationId {}",
            this.initialIterationId, this.finishIterationId);
    }
}
