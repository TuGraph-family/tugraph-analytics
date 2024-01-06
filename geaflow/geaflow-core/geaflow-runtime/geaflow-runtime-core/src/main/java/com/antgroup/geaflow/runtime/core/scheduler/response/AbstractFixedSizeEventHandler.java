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

import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import java.util.Collection;

public abstract class AbstractFixedSizeEventHandler implements IEventListener {

    protected int expectedSize;
    private IEventCompletedHandler handler;
    private ResponseEventCache eventCache;

    public AbstractFixedSizeEventHandler(int expectedSize, IEventCompletedHandler handler) {
        this.expectedSize = expectedSize;
        this.handler = handler;
        this.eventCache = buildEventCache();
    }

    @Override
    public void handleEvent(IEvent event) {
        eventCache.add(event);
        if (eventCache.size() == expectedSize) {
            if (handler != null) {
                handler.onCompleted(eventCache.values());
            }
        }
    }

    abstract ResponseEventCache buildEventCache();

    /**
     * All finished event cache.
     */
    public interface ResponseEventCache {

        /**
         * Add event to cache.
         * @param event need add to cache.
         */
        void add(IEvent event);

        /**
         * Return the cached size of current events.
         */
        int size();

        /**
         * Return all cached values.
         */
        Collection<IEvent> values();
    }

    /**
     * Callback function when all events completed as expected.
     */
    public interface IEventCompletedHandler {

        /**
         * Do callback when received all events.
         * @param events
         */
        void onCompleted(Collection<IEvent> events);
    }
}
