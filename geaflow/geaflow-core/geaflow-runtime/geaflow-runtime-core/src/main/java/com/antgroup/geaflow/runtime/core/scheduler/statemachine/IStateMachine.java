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

public interface IStateMachine<S, C> {

    /**
     * Init state machine by context.
     */
    void init(C context);

    /**
     * Check whether state machine can do transition or waiting result.
     */
    IScheduleState readyToTransition();

    /**
     * Trigger a transition to certain target state.
     */
    S transition();

    /**
     * Check whether the state machine is reach to END state.
     */
    boolean isTerminated();

    /**
     * Returns current state.
     */
    ScheduleState getCurrentState();

}
