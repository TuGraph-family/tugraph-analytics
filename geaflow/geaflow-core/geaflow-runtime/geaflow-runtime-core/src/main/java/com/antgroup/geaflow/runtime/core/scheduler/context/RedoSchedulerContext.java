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

public class RedoSchedulerContext extends AbstractCycleSchedulerContext {

    public RedoSchedulerContext(IExecutionCycle cycle, ICycleSchedulerContext parentContext) {
        super(cycle, parentContext);
    }

    @Override
    public void init() {
        super.init();
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
