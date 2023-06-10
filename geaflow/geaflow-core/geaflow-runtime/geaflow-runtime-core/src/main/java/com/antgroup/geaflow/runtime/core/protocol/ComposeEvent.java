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

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.protocol.IComposeEvent;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import java.util.List;

/**
 * An event that contains a list of basic event.
 * Suppose a cycle with one worker run the following steps one by one:
 *   firstly, worker need init runtime execution evn and
 *   then execution a round iteration and
 *   finally clean worker env.
 * Scheduler can build a {@link ComposeEvent} of {@link InitCycleEvent}, {@link ExecuteComputeEvent} and {@link CleanCycleEvent} and
 * send to worker, instead of sending three events to worker one by one.
 */
public class ComposeEvent implements IComposeEvent, ICommand {

    private int workerId;

    // A list of event that will be executed by worker sequentially.
    private List<IEvent> events;

    public ComposeEvent(int workerId, List<IEvent> events) {
        this.workerId = workerId;
        this.events = events;
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public List<IEvent> getEventList() {
        return events;
    }

    @Override
    public EventType getEventType() {
        return EventType.COMPOSE;
    }

    @Override
    public String toString() {
        return "ComposeEvent{"
            + "workerId=" + workerId
            + ", events=" + events
            + '}';
    }
}
