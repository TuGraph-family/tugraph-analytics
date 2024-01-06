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

package com.antgroup.geaflow.runtime.core.scheduler;

import com.antgroup.geaflow.cluster.common.IDispatcher;
import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.common.exception.GeaflowDispatchException;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.runtime.core.protocol.DoneEvent;
import com.antgroup.geaflow.runtime.core.scheduler.response.EventListenerKey;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerEventDispatcher implements IDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerEventDispatcher.class);

    private Map<EventListenerKey, IEventListener> listeners;
    private String cycleLogTag;

    public SchedulerEventDispatcher(String cycleLogTag) {
        this.cycleLogTag = cycleLogTag;
        this.listeners = new ConcurrentHashMap<>();
    }

    @Override
    public void dispatch(IEvent event) throws GeaflowDispatchException {
        if (event.getEventType() != EventType.DONE) {
            throw new GeaflowRuntimeException(String.format("%s not support handle event %s",
                cycleLogTag, event));
        }
        DoneEvent doneEvent = (DoneEvent) event;
        getListener(doneEvent).handleEvent(doneEvent);
    }

    public void registerListener(EventListenerKey key, IEventListener eventListener) {
        LOGGER.info("{} register event listener {}", cycleLogTag, key);
        listeners.put(key, eventListener);
    }

    public void removeListener(EventListenerKey key) {
        LOGGER.info("{} remove event listener {}", cycleLogTag, key);
        listeners.remove(key);
    }

    private IEventListener getListener(DoneEvent event) {
        IEventListener listener;
        if ((listener = listeners.get(
            EventListenerKey.of(event.getCycleId(), event.getSourceEvent(), event.getWindowId()))) != null) {
            return listener;
        } else if ((listener = listeners.get(
            EventListenerKey.of(event.getCycleId(), event.getSourceEvent()))) != null) {
            return listener;
        } else if ((listener = listeners.get(EventListenerKey.of(event.getCycleId()))) != null) {
            return listener;
        }
        throw new GeaflowRuntimeException(String.format("%s not found any listener for event %s. current listeners %s",
            cycleLogTag, event, listeners.keySet()));
    }
}
