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
public class InputMessage<T> extends AbstractMessage<PipelineMessage<T>> {

    private final PipelineMessage<T> message;
    private final long windowCount;

    public InputMessage(PipelineMessage<T> message) {
        super(message.getWindowId());
        this.message = message;
        this.windowCount = -1;
    }

    public InputMessage(long windowId, long windowCount) {
        super(windowId);
        this.message = null;
        this.windowCount = windowCount;
    }

    @Override
    public PipelineMessage<T> getMessage() {
        return message;
    }

    public long getWindowCount() {
        return windowCount;
    }

}
