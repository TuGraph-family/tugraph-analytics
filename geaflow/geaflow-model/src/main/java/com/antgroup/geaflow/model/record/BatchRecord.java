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

package com.antgroup.geaflow.model.record;

import java.io.Serializable;
import java.util.Iterator;

public class BatchRecord<T> implements Serializable {

    private final RecordArgs recordArgs;
    private Iterator<T> messageIterator;

    public BatchRecord(RecordArgs recordArgs) {
        this.recordArgs = recordArgs;
    }

    public BatchRecord(RecordArgs recordArgs, Iterator<T> messageIterator) {
        this.recordArgs = recordArgs;
        this.messageIterator = messageIterator;
    }

    public Iterator<T> getMessageIterator() {
        return messageIterator;
    }

    public RecordArgs getRecordArgs() {
        return recordArgs;
    }

    public String getStreamName() {
        return recordArgs.getName();
    }

    public long getBatchId() {
        return recordArgs.getWindowId();
    }
}
