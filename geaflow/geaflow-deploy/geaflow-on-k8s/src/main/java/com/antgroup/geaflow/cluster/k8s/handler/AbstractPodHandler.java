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

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPodHandler implements IPodEventHandler {

    protected List<IEventListener> listeners;

    public AbstractPodHandler() {
        this.listeners = new ArrayList<>();
    }

    public void addListener(IEventListener listener) {
        listeners.add(listener);
    }

    public void notifyListeners(PodEvent event) {
        for (IEventListener listener : listeners) {
            listener.onEvent(event);
        }
    }

}
