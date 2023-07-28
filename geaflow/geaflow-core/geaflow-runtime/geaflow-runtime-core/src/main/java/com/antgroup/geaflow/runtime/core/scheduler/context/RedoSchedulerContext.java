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

import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import java.util.ArrayList;
import java.util.List;

public class RedoSchedulerContext extends AbstractCycleSchedulerContext {

    public RedoSchedulerContext(IExecutionCycle cycle, ICycleSchedulerContext parentContext) {
        super(cycle, parentContext);
    }

    @Override
    public void init() {
        super.init();
        List<SchedulerState> states = new ArrayList<>();
        states.add(SchedulerState.INIT);
        if (parentContext != null) {
            // Add states of parent if not exists.
            List<SchedulerState> parentStates = parentContext.getSchedulerState(parentContext.getCurrentIterationId());
            if (parentStates != null) {
                for (SchedulerState state : parentStates) {
                    addSchedulerState(states, state);
                }
            }
        }
        // Add execute state.
        addSchedulerState(states, SchedulerState.EXECUTE);
        this.schedulerStateMap.put(getCurrentIterationId(), states);
    }

    private void addSchedulerState(List<SchedulerState> states, SchedulerState state) {
        if (!states.contains(state)) {
            states.add(state);
        }
    }

    @Override
    protected HighAvailableLevel getHaLevel() {
        return HighAvailableLevel.REDO;
    }

    @Override
    public void checkpoint(long iterationId) {
        lastCheckpointId = iterationId;
    }
}
