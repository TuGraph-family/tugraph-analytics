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

package com.antgroup.geaflow.runtime.core.scheduler.response;

import com.antgroup.geaflow.cluster.protocol.EventType;
import java.util.Objects;

public class EventListenerKey {

    private static final int DUMMY_WINDOW_ID = 0;

    private int cycleId;
    private EventType eventType;
    private long windowId;

    private EventListenerKey(int cycleId, long windowId, EventType eventType) {
        this.cycleId = cycleId;
        this.windowId = windowId;
        this.eventType = eventType;
    }

    public static EventListenerKey of(int cycleId) {
        return new EventListenerKey(cycleId, DUMMY_WINDOW_ID, null);
    }

    public static EventListenerKey of(int cycleId, EventType eventType) {
        return new EventListenerKey(cycleId, DUMMY_WINDOW_ID, eventType);
    }

    public static EventListenerKey of(int cycleId, EventType eventType, long windowId) {
        return new EventListenerKey(cycleId, windowId, eventType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventListenerKey that = (EventListenerKey) o;
        return cycleId == that.cycleId && windowId == that.windowId && eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cycleId, eventType, windowId);
    }

    @Override
    public String toString() {
        return "EventListenerKey{"
            + "cycleId=" + cycleId
            + ", eventType=" + eventType
            + ", windowId=" + windowId
            + '}';
    }
}