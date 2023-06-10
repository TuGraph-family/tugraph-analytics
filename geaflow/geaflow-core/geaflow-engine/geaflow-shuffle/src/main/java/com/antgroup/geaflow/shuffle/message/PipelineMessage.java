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

package com.antgroup.geaflow.shuffle.message;

import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;

public class PipelineMessage<T> implements PipelineEvent {

    private final RecordArgs recordArgs;
    private final IMessageIterator<T> messageIterator;

    public PipelineMessage(long batchId, String streamName, IMessageIterator<T> messageIterator) {
        this.recordArgs = new RecordArgs(batchId, streamName);
        this.messageIterator = messageIterator;
    }

    @Override
    public long getWindowId() {
        return recordArgs.getWindowId();
    }

    public IMessageIterator<T> getMessageIterator() {
        return messageIterator;
    }

    public RecordArgs getRecordArgs() {
        return recordArgs;
    }

}
