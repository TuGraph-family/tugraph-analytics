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

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;
import static com.antgroup.geaflow.ha.runtime.HighAvailableLevel.CHECKPOINT;

import com.antgroup.geaflow.cluster.common.IReliableContext;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.CheckpointUtil;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointSchedulerContext extends AbstractCycleSchedulerContext implements IReliableContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointSchedulerContext.class);

    private long checkpointDuration;
    private boolean isNeedFullCheckpoint = true;
    private transient long currentCheckpointId;

    public CheckpointSchedulerContext(IExecutionCycle cycle, ICycleSchedulerContext parentContext) {
        super(cycle, parentContext);
        if (parentContext != null) {
            throw new GeaflowRuntimeException("not support nested scheduler context fo checkpoint");
        }
        this.checkpointDuration = getConfig().getLong(BATCH_NUMBER_PER_CHECKPOINT);
    }

    @Override
    public void init() {
        super.init();
        this.schedulerStateMap.put(getCurrentIterationId(),
            Arrays.asList(SchedulerState.INIT, SchedulerState.EXECUTE));
        checkpoint(new CycleCheckpointFunction());
    }

    @Override
    public void checkpoint(long iterationId) {
        this.currentCheckpointId = iterationId;
        if (isNeedFullCheckpoint) {
            checkpoint(new CycleCheckpointFunction());
            isNeedFullCheckpoint = false;
        }
        if (CheckpointUtil.needDoCheckpoint(iterationId, checkpointDuration)) {
            checkpoint(new IterationIdCheckpointFunction());
            lastCheckpointId = iterationId;
        }
    }

    @Override
    public void load() {
    }

    /**
     * Load cycle if exists, otherwise rebuild one by input func.
     */
    public static AbstractCycleSchedulerContext build(Supplier<? extends ICycleSchedulerContext> builder) {
        AbstractCycleSchedulerContext context = loadCycle();
        long windowId = loadWindowId();
        boolean recovered = true;
        if (context == null) {
            Preconditions.checkArgument(builder != null, "should provide function to build new context");
            context = (AbstractCycleSchedulerContext) builder.get();
            if (context == null) {
                throw new GeaflowRuntimeException("build new context failed");
            }
            recovered = false;
        }
        if (context.getHaLevel() == CHECKPOINT) {
            context.init(windowId);

            if (recovered) {
                LOGGER.info("rollback");
                context.schedulerStateMap.put(context.getCurrentIterationId(),
                    Arrays.asList(SchedulerState.ROLLBACK, SchedulerState.EXECUTE));
            } else {
                LOGGER.info("init and rollback");
                context.schedulerStateMap.put(context.getCurrentIterationId(),
                    Arrays.asList(SchedulerState.INIT, SchedulerState.ROLLBACK, SchedulerState.EXECUTE));
            }
        }
        return context;
    }

    private static long loadWindowId() {
        Long lastWindowId = ClusterMetaStore.getInstance().getWindowId();
        long windowId;
        if (lastWindowId == null) {
            windowId = CheckpointSchedulerContext.DEFAULT_INITIAL_ITERATION_ID;
            LOGGER.info("not found last success batchId, set startIterationId to {}", windowId);
        } else {
            windowId = lastWindowId + 1;
            LOGGER.info("load scheduler context, lastWindowId {}, current start windowId {}",
                lastWindowId, windowId);
        }
        return windowId;
    }

    private static CheckpointSchedulerContext loadCycle() {
        CheckpointSchedulerContext context = (CheckpointSchedulerContext) ClusterMetaStore.getInstance().getCycle();
        if (context == null) {
            LOGGER.info("not found recoverable cycle");
            return null;
        }
        return context;
    }

    @Override
    public void checkpoint(IReliableContextCheckpointFunction function) {
        function.doCheckpoint(this);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    protected HighAvailableLevel getHaLevel() {
        return CHECKPOINT;
    }

    public class IterationIdCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            long checkpointId = ((CheckpointSchedulerContext) context).currentCheckpointId;
            ClusterMetaStore.getInstance().saveWindowId(checkpointId);
            LOGGER.info("cycle {} do checkpoint {}",
                ((CheckpointSchedulerContext) context).getCycle().getCycleId(), checkpointId);
        }
    }

    public class CycleCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            long checkpointId = ((CheckpointSchedulerContext) context).currentCheckpointId;
            ClusterMetaStore.getInstance().saveCycle(context).flush();
            LOGGER.info("cycle {} do checkpoint {} for full context",
                ((CheckpointSchedulerContext) context).getCycle().getCycleId(), checkpointId);
        }
    }
}
