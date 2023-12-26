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

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.runtime.core.protocol.DoneEvent;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SourceFinishResponseEventListener extends AbstractFixedSizeEventHandler {

    private int eventCount;

    public SourceFinishResponseEventListener(int eventCount, IEventCompletedHandler handler) {
        super(eventCount, handler);
        this.eventCount = eventCount;
    }

    @Override
    ResponseEventCache buildEventCache() {
        return new EventCache(eventCount);
    }

    public class EventCache implements ResponseEventCache {

        private Map<Integer, IEvent> events;

        public EventCache(int capacity) {
            this.events = new ConcurrentHashMap<>(capacity);
        }

        @Override
        public void add(IEvent event) {
            int taskId = ((DoneEvent) event).getTaskId();
            if (!events.containsKey(taskId)) {
                events.put(taskId, event);
            }
        }

        @Override
        public int size() {
            return events.size();
        }

        @Override
        public Collection<IEvent> values() {
            return events.values();
        }
    }
}
