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

package com.antgroup.geaflow.cluster.k8s.handler;

import com.antgroup.geaflow.common.config.Configuration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PodHandlerRegistry {

    private final Map<EventKind, IPodEventHandler> eventHandlerMap;
    private static PodHandlerRegistry INSTANCE;

    private PodHandlerRegistry(Configuration configuration) {
        this.eventHandlerMap = new HashMap<>();
        this.eventHandlerMap.put(EventKind.OOM, new PodOOMHandler()) ;
        this.eventHandlerMap.put(EventKind.EVICTION, new PodEvictHandler(configuration));
    }

    public static synchronized PodHandlerRegistry getInstance(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new PodHandlerRegistry(configuration);
        }
        return INSTANCE;
    }

    public Collection<IPodEventHandler> getHandlers() {
        return eventHandlerMap.values();
    }

    public enum EventKind {
        OOM,
        EVICTION
    }

}
