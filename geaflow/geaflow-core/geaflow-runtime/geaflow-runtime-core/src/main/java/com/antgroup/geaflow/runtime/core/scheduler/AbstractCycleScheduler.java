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

import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.result.ExecutionResult;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCycleScheduler<R, E> implements ICycleScheduler<R, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCycleScheduler.class);

    protected IExecutionCycle cycle;
    protected ICycleSchedulerContext context;

    @Override
    public void init(ICycleSchedulerContext context) {
        this.cycle = context.getCycle();
        this.context = context;
    }

    public IExecutionResult<R, E> execute() {

        String cycleLogTag = LoggerFormatter.getCycleTag(cycle.getPipelineName(), cycle.getCycleId());
        try {
            long finishedIterationId = 0;
            while (!context.isCycleFinished()) {

                // Submit tasks to cycle head until run out of on the fly permits.
                while (context.hasNextIteration()) {
                    long iterationId = context.getNextIterationId();
                    execute(iterationId);
                }

                // Handle response task until received all responses of certain iteration.
                while (context.hasNextToFinish()) {
                    finishedIterationId = context.getNextFinishIterationId();
                    finish(finishedIterationId);
                    context.finish(finishedIterationId);
                }
            }
            LOGGER.info("{} finished at {}", cycleLogTag, finishedIterationId);
            R result = finish();
            context.finish();
            return ExecutionResult.buildSuccessResult(result);
        } catch (Throwable e) {
            LOGGER.error(String.format("%s occur exception", cycleLogTag), e);
            return ExecutionResult.buildFailedResult(e);
        }
    }

    @Override
    public void close() {
    }

    protected abstract void execute(long iterationId);

    protected abstract void finish(long iterationId);

    protected abstract R finish();
}
