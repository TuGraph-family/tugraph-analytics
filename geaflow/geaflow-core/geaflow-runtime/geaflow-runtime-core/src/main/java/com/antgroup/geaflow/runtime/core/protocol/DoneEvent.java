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
import com.antgroup.geaflow.cluster.protocol.ICycleResponseEvent;
import com.antgroup.geaflow.common.metric.EventMetrics;

/**
 * Defined the end of one iteration.
 * It sent from cycle tail tasks to scheduler.
 */
public class DoneEvent<T> implements ICycleResponseEvent {

    // Cycle id of the current event.
    private int cycleId;

    // Window id of cycle.
    private long windowId;

    // The task id of cycle tail that send back event to scheduler.
    private int taskId;

    // Event that trigger the execution.
    private EventType sourceEvent;

    // Result of execution. null if no need result.
    private T result;

    private EventMetrics eventMetrics;

    public DoneEvent(int cycleId, long windowId, int tailTaskId, EventType sourceEvent) {
        this(cycleId, windowId, tailTaskId, sourceEvent, null, null);
    }

    public DoneEvent(int cycleId, long windowId, int tailTaskId, EventType sourceEvent, T result) {
        this(cycleId, windowId, tailTaskId, sourceEvent, result, null);
    }

    public DoneEvent(int cycleId,
                     long windowId,
                     int tailTaskId,
                     EventType sourceEvent,
                     T result,
                     EventMetrics eventMetrics) {
        this.cycleId = cycleId;
        this.windowId = windowId;
        this.taskId = tailTaskId;
        this.sourceEvent = sourceEvent;
        this.result = result;
        this.eventMetrics = eventMetrics;
    }

    @Override
    public int getCycleId() {
        return cycleId;
    }

    public long getWindowId() {
        return windowId;
    }

    public int getTaskId() {
        return taskId;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public EventType getSourceEvent() {
        return sourceEvent;
    }

    @Override
    public EventType getEventType() {
        return EventType.DONE;
    }

    public EventMetrics getEventMetrics() {
        return eventMetrics;
    }

    public void setEventMetrics(EventMetrics eventMetrics) {
        this.eventMetrics = eventMetrics;
    }

    @Override
    public String toString() {
        return "DoneEvent{"
            + "cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", taskId=" + taskId
            + ", sourceEvent=" + sourceEvent
            + '}';
    }
}
