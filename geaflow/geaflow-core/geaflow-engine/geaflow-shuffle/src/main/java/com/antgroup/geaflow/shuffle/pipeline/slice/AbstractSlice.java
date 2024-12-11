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

package com.antgroup.geaflow.shuffle.pipeline.slice;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import java.util.ArrayDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSlice implements IPipelineSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineSlice.class);

    protected final SliceId sliceId;
    protected final String taskLogTag;
    protected int refCount;
    protected int totalBufferCount;
    protected ArrayDeque<PipeBuffer> buffers;
    protected PipelineSliceReader sliceReader;
    protected volatile boolean isReleased;

    public AbstractSlice(String taskLogTag, SliceId sliceId, int refCount) {
        this.sliceId = sliceId;
        this.taskLogTag = taskLogTag;
        this.refCount = refCount;
        this.totalBufferCount = 0;
        this.buffers = new ArrayDeque<>();
    }

    @Override
    public SliceId getSliceId() {
        return sliceId;
    }

    @Override
    public int getTotalBufferCount() {
        return totalBufferCount;
    }

    @Override
    public PipelineSliceReader createSliceReader(long startBatchId, PipelineSliceListener listener) {
        synchronized (buffers) {
            if (isReleased) {
                throw new GeaflowRuntimeException("slice is released:" + sliceId);
            }
            if (sliceReader != null && sliceReader.hasNext()) {
                throw new GeaflowRuntimeException("slice is already created:" + sliceId);
            }

            refCount--;
            LOGGER.info("creating reader for {} {} with startBatch:{} refCount:{}",
                taskLogTag, sliceId, startBatchId, refCount);

            // multiple repeatable readers can exist at the same time.
            if (refCount >= 1) {
                sliceReader = new RepeatableSliceReader(this, startBatchId, listener);
            } else {
                sliceReader = new DisposableSliceReader(this, startBatchId, listener);
            }
            return sliceReader;
        }
    }

    @Override
    public boolean canRelease() {
        return refCount == 0 && !hasNext();
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

}
