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

package com.antgroup.geaflow.runtime.core.scheduler.statemachine;

import com.antgroup.geaflow.cluster.protocol.ScheduleStateType;
import java.util.List;

public class ComposeState implements IScheduleState {

    private List<IScheduleState> states;

    public ComposeState(List<IScheduleState> states) {
        this.states = states;
    }

    public static ComposeState of(List<IScheduleState> states) {
        return new ComposeState(states);
    }

    public List<IScheduleState> getStates() {
        return states;
    }

    @Override
    public ScheduleStateType getScheduleStateType() {
        return ScheduleStateType.COMPOSE;
    }

    @Override
    public String toString() {
        return "compose{"
            + states
            + '}';
    }

}
