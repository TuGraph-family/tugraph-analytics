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

package com.antgroup.geaflow.shuffle.api.pipeline.buffer;

import java.io.Serializable;

public class PipeBuffer implements Serializable {

    private final OutBuffer buffer;
    private final boolean isData;
    private final long batchId;
    private final int count;
    private final boolean isFinish;

    public PipeBuffer(byte[] buffer, long batchId, boolean isData) {
        this.buffer = new HeapBuffer(buffer, true);
        this.batchId = batchId;
        this.isData = isData;
        this.count = 0;
        this.isFinish = false;
    }

    public PipeBuffer(OutBuffer buffer, long batchId, boolean isData) {
        this.buffer = buffer;
        this.batchId = batchId;
        this.isData = isData;
        this.count = 0;
        this.isFinish = false;
    }

    public PipeBuffer(long batchId, int count, boolean isData, boolean isFinish) {
        this.buffer = null;
        this.batchId = batchId;
        this.isData = isData;
        this.count = count;
        this.isFinish = isFinish;
    }

    public OutBuffer getBuffer() {
        return buffer;
    }

    public int getBufferSize() {
        return buffer != null ? buffer.getBufferSize() : 0;
    }

    public boolean isData() {
        return isData;
    }

    public long getBatchId() {
        return batchId;
    }

    public int getCount() {
        return count;
    }

    public boolean isFinish() {
        return isFinish;
    }
}
