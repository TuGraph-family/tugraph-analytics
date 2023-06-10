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

package com.antgroup.geaflow.cluster.protocol;

import com.antgroup.geaflow.shuffle.message.PipelineMessage;

/**
 * A message which is processed by worker.
 */
public class Message<T> implements IMessage {

    private final long windowId;
    private PipelineMessage message;
    private long windowCount;

    public Message(long windowId, PipelineMessage message) {
        this.windowId = windowId;
        this.message = message;
    }

    public Message(long windowId, long windowCount) {
        this.windowId = windowId;
        this.windowCount = windowCount;
    }

    public long getWindowId() {
        return windowId;
    }

    public PipelineMessage getMessage() {
        return message;
    }

    public long getWindowCount() {
        return windowCount;
    }

    @Override
    public EventType getEventType() {
        return EventType.MESSAGE;
    }
}
