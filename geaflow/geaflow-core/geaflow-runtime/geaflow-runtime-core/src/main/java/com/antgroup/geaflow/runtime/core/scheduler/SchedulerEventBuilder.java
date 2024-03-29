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

import static com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext.DEFAULT_INITIAL_ITERATION_ID;

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.protocol.ScheduleStateType;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.protocol.CleanCycleEvent;
import com.antgroup.geaflow.runtime.core.protocol.ComposeEvent;
import com.antgroup.geaflow.runtime.core.protocol.ExecuteComputeEvent;
import com.antgroup.geaflow.runtime.core.protocol.ExecuteFirstIterationEvent;
import com.antgroup.geaflow.runtime.core.protocol.FinishIterationEvent;
import com.antgroup.geaflow.runtime.core.protocol.InitCollectCycleEvent;
import com.antgroup.geaflow.runtime.core.protocol.InitCycleEvent;
import com.antgroup.geaflow.runtime.core.protocol.InitIterationEvent;
import com.antgroup.geaflow.runtime.core.protocol.InterruptTaskEvent;
import com.antgroup.geaflow.runtime.core.protocol.IterationExecutionComputeWithAggEvent;
import com.antgroup.geaflow.runtime.core.protocol.LaunchSourceEvent;
import com.antgroup.geaflow.runtime.core.protocol.LoadGraphProcessEvent;
import com.antgroup.geaflow.runtime.core.protocol.PopWorkerEvent;
import com.antgroup.geaflow.runtime.core.protocol.RollbackCycleEvent;
import com.antgroup.geaflow.runtime.core.protocol.StashWorkerEvent;
import com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.CollectExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.antgroup.geaflow.runtime.core.scheduler.io.IoDescriptorBuilder;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class SchedulerEventBuilder {

    private static final int COMPUTE_FETCH_COUNT = 1;

    private ICycleSchedulerContext context;
    private ExecutionNodeCycle cycle;
    private DataExchangeMode outputExchangeMode;
    private CycleResultManager resultManager;
    private boolean enableAffinity;
    private boolean isIteration;
    private long schedulerId;

    public SchedulerEventBuilder(long schedulerId,
                                 ICycleSchedulerContext context,
                                 DataExchangeMode outputExchangeMode,
                                 CycleResultManager resultManager) {
        this.schedulerId = schedulerId;
        this.context = context;
        this.cycle = (ExecutionNodeCycle) context.getCycle();
        this.outputExchangeMode = outputExchangeMode;
        this.resultManager = resultManager;
        this.enableAffinity = ((AbstractCycleSchedulerContext) context).getParentContext() != null
            && ((AbstractCycleSchedulerContext) context).getParentContext().getCycle().getIterationCount() > 1;
        this.isIteration = cycle.getVertexGroup().getCycleGroupMeta().isIterative();
    }

    public Map<Integer, IEvent> build(ScheduleStateType state, long iterationId) {
        switch (state) {
            case INIT:
                return buildInit();
            case ITERATION_INIT:
                return buildInitIteration(iterationId);
            case EXECUTE_COMPUTE:
                return buildExecute(iterationId);
            case ITERATION_FINISH:
                return finishIteration();
            case CLEAN_CYCLE:
                return finishPipeline();
            case ROLLBACK:
                return handleRollback();
            default:
                throw new GeaflowRuntimeException(String.format("not support event %s yet", state));
        }

    }

    /**
     * Build assign event for all tasks.
     */
    private Map<Integer, IEvent> buildInit() {
        if (this.cycle.getType() == ExecutionCycleType.ITERATION) {
            return buildInitIteration();
        } else {
            return buildInitPipeline();
        }
    }

    private Map<Integer, IEvent> buildInitPipeline() {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : this.cycle.getTasks()) {
            IoDescriptor ioDescriptor =
                IoDescriptorBuilder.buildPipelineIoDescriptor(task, this.cycle, this.resultManager);
            events.put(task.getTaskId(), buildInitOrPopEvent(task, ioDescriptor));
        }
        return events;
    }

    private Map<Integer, IEvent> buildInitIteration() {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : this.cycle.getTasks()) {
            IoDescriptor ioDescriptor =
                IoDescriptorBuilder.buildPipelineIoDescriptor(task, this.cycle, this.resultManager);
            events.put(task.getTaskId(), buildInitOrPopEvent(task, ioDescriptor));
        }
        return events;
    }

    private IEvent buildInitOrPopEvent(ExecutionTask task, IoDescriptor ioDescriptor) {
        if (!cycle.isWorkerAssigned()) {
            InitCycleEvent initEvent = buildInitCycleEvent(task);
            initEvent.setIoDescriptor(ioDescriptor);
            return initEvent;

        } else {
            PopWorkerEvent popWorkerEvent = new PopWorkerEvent(schedulerId,
                task.getWorkerInfo().getWorkerIndex(),
                cycle.getCycleId(),
                context.getInitialIterationId(), cycle.getPipelineId(), cycle.getPipelineName(), task.getTaskId());
            popWorkerEvent.setIoDescriptor(ioDescriptor);
            return popWorkerEvent;
        }
    }

    private InitCycleEvent buildInitCycleEvent(ExecutionTask task) {
        InitCycleEvent init;
        int workerId = task.getWorkerInfo().getWorkerIndex();
        HighAvailableLevel highAvailableLevel = cycle.getHighAvailableLevel();
        if (this.cycle.getType() == ExecutionCycleType.ITERATION) {
            highAvailableLevel = HighAvailableLevel.REDO;
        }

        if (cycle instanceof CollectExecutionNodeCycle) {
            init = new InitCollectCycleEvent(schedulerId, workerId, cycle.getCycleId(),
                context.getInitialIterationId(), cycle.getPipelineId(), cycle.getPipelineName(),
                task, highAvailableLevel, context.getInitialIterationId());

        } else {
            init = new InitCycleEvent(schedulerId, workerId, cycle.getCycleId(),
                context.getInitialIterationId(), cycle.getPipelineId(), cycle.getPipelineName(),
                task, highAvailableLevel, context.getInitialIterationId());
        }

        init.setDriverId(cycle.getDriverId());
        return init;
    }

    /**
     * Build launch for all cycle heads.
     */
    private Map<Integer, IEvent> buildExecute(long iterationId) {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : cycle.getTasks()) {
            if (ExecutionTaskUtils.isCycleHead(task)) {
                // Only submit launch to cycle head.
                long fetchId = iterationId;
                // Fetch previous iteration input.
                if (isIteration) {
                    if (iterationId > DEFAULT_INITIAL_ITERATION_ID) {
                        fetchId = iterationId - 1;
                    } else {
                        fetchId = context.getInitialIterationId();
                    }
                }
                IEvent event = buildExecute(task, task.getWorkerInfo().getWorkerIndex(),
                    cycle.getCycleId(), iterationId, fetchId);
                events.put(task.getTaskId(), event);
            } else if (iterationId == context.getInitialIterationId()) {
                // Build execute compute for non-tail event during first window.
                int workerId = task.getWorkerInfo().getWorkerIndex();
                ExecuteComputeEvent execute = new ExecuteComputeEvent(schedulerId, workerId,
                    cycle.getCycleId(), context.getInitialIterationId(),
                    context.getInitialIterationId(),
                    context.getFinishIterationId() - context.getInitialIterationId() + 1,
                    cycle.getIterationCount() > 1);
                events.put(task.getTaskId(), execute);

            }
        }
        return events;
    }

    private Map<Integer, IEvent> buildInitIteration(long iterationId) {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : cycle.getTasks()) {
            if (ExecutionTaskUtils.isCycleHead(task)) {
                int workerId = task.getWorkerInfo().getWorkerIndex();
                if (cycle.getVertexGroup().getParentVertexGroupIds().isEmpty()
                    && ExecutionTaskUtils.isCycleHead(task)) {
                    LaunchSourceEvent launchSourceEvent = new LaunchSourceEvent(schedulerId, workerId,
                        cycle.getCycleId(), iterationId);
                    events.put(task.getTaskId(), launchSourceEvent);
                } else {
                    // Load graph.
                    IEvent loadGraph = new LoadGraphProcessEvent(schedulerId, workerId, cycle.getCycleId(),
                        iterationId, context.getInitialIterationId(), COMPUTE_FETCH_COUNT);
                    // Init iteration.
                    InitIterationEvent iterationInit = new InitIterationEvent(schedulerId, workerId,
                        cycle.getCycleId(),
                        iterationId, cycle.getPipelineId(), cycle.getPipelineName());
                    iterationInit.setIoDescriptor(new IoDescriptor(
                        IoDescriptorBuilder.buildIterationInitInputDescriptor(task, this.cycle,
                            resultManager),
                        null));
                    IEvent execute = new ExecuteFirstIterationEvent(schedulerId, workerId,
                        cycle.getCycleId(), iterationId);
                    ComposeEvent composeEvent = new ComposeEvent(workerId,
                        Arrays.asList(loadGraph, iterationInit, execute));
                    events.put(task.getTaskId(), composeEvent);
                }
            }
        }
        return events;
    }

    private Map<Integer, IEvent> handleRollback() {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : this.cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            // Do not do rollback if recover from initial iteration id.
            if (context.getCurrentIterationId() != DEFAULT_INITIAL_ITERATION_ID) {
                RollbackCycleEvent rollbackCycleEvent = new RollbackCycleEvent(schedulerId, workerId,
                    this.cycle.getCycleId(),
                    context.getCurrentIterationId() - 1);
                events.put(task.getTaskId(), rollbackCycleEvent);
            }
        }
        return events;
    }

    private Map<Integer, IEvent> finishPipeline() {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        boolean needInterrupt = context.getCurrentIterationId() < context.getFinishIterationId();
        for (ExecutionTask task : cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            IEvent cleanEvent;
            if (enableAffinity) {
                cleanEvent = new StashWorkerEvent(schedulerId, workerId, cycle.getCycleId(), cycle.getIterationCount(), task.getTaskId());
            } else {
                cleanEvent = new CleanCycleEvent(schedulerId, workerId, cycle.getCycleId(), cycle.getIterationCount());
            }
            if (needInterrupt && context.getCycle().getType() != ExecutionCycleType.ITERATION
                && context.getCycle().getType() != ExecutionCycleType.ITERATION_WITH_AGG) {
                InterruptTaskEvent interruptTaskEvent = new InterruptTaskEvent(workerId, cycle.getCycleId());
                ComposeEvent composeEvent =
                    new ComposeEvent(task.getWorkerInfo().getWorkerIndex(),
                        Arrays.asList(interruptTaskEvent, cleanEvent));
                events.put(task.getTaskId(), composeEvent);
            } else {
                events.put(task.getTaskId(), cleanEvent);
            }
        }
        return events;
    }

    private Map<Integer, IEvent> finishIteration() {
        Map<Integer, IEvent> events = new LinkedHashMap<>();
        for (ExecutionTask task : cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            // Finish iteration
            FinishIterationEvent iterationFinishEvent = new FinishIterationEvent(schedulerId,
                workerId, context.getInitialIterationId(), cycle.getCycleId(), task.getTaskId());

            events.put(task.getTaskId(), iterationFinishEvent);
        }
        return events;
    }

    private IEvent buildExecute(ExecutionTask task, int workerId, int cycleId, long iterationId, long fetchId) {
        if (cycle.getVertexGroup().getParentVertexGroupIds().isEmpty() && ExecutionTaskUtils.isCycleHead(task)) {
            return new LaunchSourceEvent(schedulerId, workerId, cycleId, iterationId);
        } else {
            // TODO remove init iteration during trigger
            //      after worker fully support handle load graph and init iteration.
            InputDescriptor inputDescriptor = IoDescriptorBuilder.buildIterationExecuteInputDescriptor(task,
                this.cycle, resultManager);
            if (inputDescriptor.getInputDescMap().isEmpty()) {
                return new ExecuteComputeEvent(schedulerId, workerId, cycleId, iterationId, fetchId, COMPUTE_FETCH_COUNT);
            } else {
                return new IterationExecutionComputeWithAggEvent(schedulerId, workerId, cycleId,
                    iterationId, fetchId, COMPUTE_FETCH_COUNT, inputDescriptor);
            }
        }
    }
}
