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

import static com.antgroup.geaflow.cluster.protocol.ScheduleStateType.END;

import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.result.ExecutionResult;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.IScheduleState;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.IStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCycleScheduler<R, E> implements ICycleScheduler<R, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCycleScheduler.class);

    protected IExecutionCycle cycle;
    protected ICycleSchedulerContext context;
    protected SchedulerEventDispatcher dispatcher;
    protected IStateMachine stateMachine;

    @Override
    public void init(ICycleSchedulerContext context) {
        this.cycle = context.getCycle();
        this.context = context;
    }

    public IExecutionResult<R, E> execute() {

        String cycleLogTag = LoggerFormatter.getCycleTag(cycle.getPipelineName(), cycle.getCycleId());
        try {
            while (!stateMachine.isTerminated()) {
                while (true) {
                    IScheduleState oldState = stateMachine.getCurrentState();
                    IScheduleState state = stateMachine.readyToTransition();
                    if (state == null) {
                        finishFlyingEvent();
                        break;
                    }
                    if (state.getScheduleStateType() == END) {
                        break;
                    }
                    LOGGER.info("{} state transition from {} to {}",
                        this.getClass(), oldState.getScheduleStateType(), state.getScheduleStateType());
                    execute(state);
                }
            }
            LOGGER.info("{} finished at {}", cycleLogTag, context.getFinishIterationId());
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

    protected abstract void execute(IScheduleState state);

    protected void finishFlyingEvent() {
        // Handle response task until received all responses of certain iteration.
        while (context.hasNextToFinish()) {
            long finishedIterationId = context.getNextFinishIterationId();
            finish(finishedIterationId);
            context.finish(finishedIterationId);
        }
    }

    protected abstract void finish(long iterationId);

    protected abstract R finish();

    protected abstract void registerEventListener();
}
