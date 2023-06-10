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

package com.antgroup.geaflow.cluster.driver;

import com.antgroup.geaflow.cluster.common.IDispatcher;
import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import java.util.ArrayList;
import java.util.List;

public class DriverEventDispatcher implements IDispatcher {

    private List<IEventListener> eventListenerList;

    public DriverEventDispatcher() {
        this.eventListenerList = new ArrayList<>();
    }

    public void dispatch(IEvent event) {
        for (IEventListener eventHandler : eventListenerList) {
            eventHandler.handleEvent(event);
        }
    }

    public void registerListener(IEventListener eventListener) {
        this.eventListenerList.add(eventListener);
    }

    public void removeListener(IEventListener eventListener) {
        this.eventListenerList.remove(eventListener);
    }
}
