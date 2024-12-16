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
import com.antgroup.geaflow.runtime.core.scheduler.ExecutableEventIterator.ExecutableEvent;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.antgroup.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface ICycleSchedulerContext<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>> extends Serializable {

    /**
     * Returns execution cycle.
     */
    C getCycle();

    PCC getParentContext();

    /**
     * Returns execution config.
     */
    Configuration getConfig();

    /**
     * Returns whether cycle is finished.
     */
    boolean isCycleFinished();

    /**
     * Returns when cycle is recovered.
     */
    boolean isRecovered();

    /**
     * Returns whether cycle need rollback.
     */
    boolean isRollback();

    /**
     * Returns whether enable prefetch.
     */
    boolean isPrefetch();

    /**
     * Returns current iteration id.
     */
    long getCurrentIterationId();

    /**
     * Returns finish iteration id.
     */
    long getFinishIterationId();

    /**
     * Check whether has next iteration.
     */
    boolean hasNextIteration();

    /**
     * Returns next iteration id.
     */
    long getNextIterationId();

    /**
     * Check whether has next cycle to finish.
     */
    boolean hasNextToFinish();

    /**
     * Returns next finish iteration id.
     */
    long getNextFinishIterationId();

    /**
     * Returns initial iteration id.
     */
    long getInitialIterationId();

    /**
     * Returns cycle result manager.
     */
    CycleResultManager getResultManager();

    /**
     * Assign workers for cycle.
     */
    List<WorkerInfo> assign(C cycle);

    /**
     * Release worker for cycle.
     */
    void release(C cycle);

    /**
     * Finish the windowId iteration.
     */
    void finish(long windowId);

    /**
     * Finish cycle.
     */
    void finish();

    /**
     * Close workerManager.
     */
    void close(IExecutionCycle cycle);

    /**
     * Returns scheduler worker manager.
     */
    IScheduledWorkerManager<C> getSchedulerWorkerManager();

    /**
     * Returns prefetch events needed to be finished.
     */
    Map<Integer, ExecutableEvent> getPrefetchEvents();

    enum SchedulerState {
        /**
         * Init state.
         */
        INIT,
        /**
         * Execute state.
         */
        EXECUTE,
        /**
         * Finish state.
         */
        FINISH,
        /**
         * Rollback state.
         */
        ROLLBACK,
    }

}
