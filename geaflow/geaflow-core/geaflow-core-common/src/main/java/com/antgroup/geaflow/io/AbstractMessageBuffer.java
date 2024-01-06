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

package com.antgroup.geaflow.io;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.EventMetrics;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AbstractMessageBuffer<R> implements IMessageBuffer<R> {

    private static final int DEFAULT_TIMEOUT_MS = 100;

    private final LinkedBlockingQueue<R> queue;
    protected volatile EventMetrics eventMetrics;

    public AbstractMessageBuffer(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public void offer(R record) {
        while (true) {
            try {
                if (this.queue.offer(record, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                throw new GeaflowRuntimeException(e);
            }
        }
    }

    @Override
    public R poll(long timeout, TimeUnit unit) {
        try {
            return this.queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public void setEventMetrics(EventMetrics eventMetrics) {
        this.eventMetrics = eventMetrics;
    }

    public EventMetrics getEventMetrics() {
        return this.eventMetrics;
    }

}
