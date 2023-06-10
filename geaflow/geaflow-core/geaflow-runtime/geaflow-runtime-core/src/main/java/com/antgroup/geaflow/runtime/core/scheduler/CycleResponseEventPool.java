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

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CycleResponseEventPool<T> {

    private LinkedBlockingQueue<T> eventQueue;
    private static final int WAITING_TIME_OUT = 10;

    public CycleResponseEventPool() {
        eventQueue = new LinkedBlockingQueue();
    }

    public void notifyEvent(T event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public T waitEvent() {
        while (true) {
            try {
                // Wait until get available response.
                T event = eventQueue.poll(WAITING_TIME_OUT, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                return event;
            } catch (InterruptedException e) {
                throw new GeaflowRuntimeException(e);
            }
        }
    }

    public void clear() {
        eventQueue.clear();
    }
}
