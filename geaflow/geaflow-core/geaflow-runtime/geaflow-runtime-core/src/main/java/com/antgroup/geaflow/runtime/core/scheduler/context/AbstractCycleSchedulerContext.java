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

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.pipeline.callback.ICallbackFunction;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.antgroup.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;
import com.antgroup.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCycleSchedulerContext implements ICycleSchedulerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCycleSchedulerContext.class);

    public static final long DEFAULT_INITIAL_ITERATION_ID = 1;

    protected IExecutionCycle cycle;
    protected transient AtomicLong iterationIdGenerator;
    protected transient Queue<Long> flyingIterations;
    protected transient long currentIterationId;
    protected long finishIterationId;
    protected transient long initialIterationId;
    protected transient Map<Long, List<SchedulerState>> schedulerStateMap;
    protected transient long lastCheckpointId;
    protected transient long terminateIterationId;

    protected transient ICycleSchedulerContext parentContext;
    protected IScheduledWorkerManager workerManager;
    protected transient CycleResultManager cycleResultManager;
    protected ICallbackFunction callbackFunction;

    public AbstractCycleSchedulerContext(IExecutionCycle cycle, ICycleSchedulerContext parentContext) {
        this.cycle = cycle;
        this.parentContext = parentContext;

        if (parentContext != null) {
            // Get worker manager from parent context, no need to init.
            this.workerManager = parentContext.getSchedulerWorkerManager();
        } else {
            this.workerManager = ScheduledWorkerManagerFactory.createScheduledWorkerManager(cycle.getConfig(),
                getHaLevel());
        }
    }

    public void init() {
        long startIterationId;
        if (parentContext != null) {
            startIterationId = parentContext.getCurrentIterationId();
        } else {
            startIterationId = DEFAULT_INITIAL_ITERATION_ID;
        }
        this.finishIterationId = cycle.getIterationCount() == Long.MAX_VALUE
            ? cycle.getIterationCount() : cycle.getIterationCount() + startIterationId - 1;
        init(startIterationId);
    }

    public void init(long startIterationId) {
        this.flyingIterations = new LinkedBlockingQueue<>(cycle.getFlyingCount());
        this.currentIterationId =  startIterationId;
        this.initialIterationId = startIterationId;
        this.iterationIdGenerator = new AtomicLong(currentIterationId);
        this.lastCheckpointId = 0;
        this.terminateIterationId = Long.MAX_VALUE;
        this.schedulerStateMap = new HashMap<>();
        if (parentContext != null) {
            this.cycleResultManager = parentContext.getResultManager();
        } else {
            this.cycleResultManager = new CycleResultManager();
        }

        this.workerManager.init(cycle);

        LOGGER.info("{} init cycle context onTheFlyThreshold {}, currentIterationId {}, "
                + "iterationCount {}, finishIterationId {}, initialIterationId {}",
            cycle.getPipelineName(), cycle.getFlyingCount(), this.currentIterationId,
            cycle.getIterationCount(), this.finishIterationId, this.initialIterationId);

    }

    @Override
    public IExecutionCycle getCycle() {
        return this.cycle;
    }

    @Override
    public Configuration getConfig() {
        return cycle.getConfig();
    }

    @Override
    public boolean isCycleFinished() {
        return (iterationIdGenerator.get() > finishIterationId || lastCheckpointId >= terminateIterationId)
            && flyingIterations.isEmpty();
    }

    @Override
    public long getCurrentIterationId() {
        return currentIterationId;
    }

    @Override
    public long getFinishIterationId() {
        return finishIterationId;
    }

    @Override
    public boolean hasNextIteration() {
        return iterationIdGenerator.get() <= finishIterationId && lastCheckpointId < terminateIterationId
            && flyingIterations.size() < cycle.getFlyingCount();
    }

    @Override
    public long getNextIterationId() {
        long iterationId = iterationIdGenerator.getAndIncrement();
        flyingIterations.add(iterationId);
        this.currentIterationId = iterationId;
        return iterationId;
    }

    @Override
    public boolean hasNextToFinish() {
        return !flyingIterations.isEmpty() && !hasNextIteration();
    }

    @Override
    public long getNextFinishIterationId() {
        return flyingIterations.remove();
    }

    @Override
    public long getInitialIterationId() {
        return initialIterationId;
    }

    @Override
    public List<SchedulerState> getSchedulerState(long iterationId) {
        return schedulerStateMap.get(iterationId);
    }

    @Override
    public IScheduledWorkerManager getSchedulerWorkerManager() {
        return workerManager;
    }

    @Override
    public CycleResultManager getResultManager() {
        return cycleResultManager;
    }

    public ICycleSchedulerContext getParentContext() {
        return parentContext;
    }

    public void setTerminateIterationId(long iterationId) {
        terminateIterationId = iterationId;
    }

    public void setCallbackFunction(ICallbackFunction callbackFunction) {
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void finish(long windowId) {
        if (callbackFunction != null) {
            callbackFunction.window(windowId);
        }
        checkpoint(windowId);
    }
    
    @Override
    public void finish() {
        if (callbackFunction != null) {
            callbackFunction.terminal();
        }
    }

    @Override
    public List<WorkerInfo> assign(IExecutionCycle cycle) {
        return workerManager.assign(cycle);
    }

    @Override
    public void release(IExecutionCycle cycle) {
        workerManager.release(cycle);
    }

    @Override
    public void close() {
        workerManager.close();
    }

    abstract void checkpoint(long windowId);

    abstract HighAvailableLevel getHaLevel();

    @VisibleForTesting
    public Map<Long, List<SchedulerState>> getSchedulerStateMap() {
        return schedulerStateMap;
    }
}
