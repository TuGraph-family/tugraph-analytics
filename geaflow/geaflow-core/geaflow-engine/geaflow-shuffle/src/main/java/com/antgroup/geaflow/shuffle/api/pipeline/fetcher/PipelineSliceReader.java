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

package com.antgroup.geaflow.shuffle.api.pipeline.fetcher;

import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeChannelBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Called by {@link SequenceSliceReader} for remote consumption
 * and {@link com.antgroup.geaflow.shuffle.api.pipeline.channel.LocalInputChannel}
 * for local consumption.
 */
public class PipelineSliceReader {

    private final PipelineSlice slice;
    private final PipelineSliceListener listener;
    private final AtomicBoolean released;
    private final boolean disposable;
    private final Iterator<PipeBuffer> sliceIterator;

    private int totalMessages;
    private int consumedMessages;

    // Client request batch id.
    private volatile long requestBatchId;
    private volatile long consumedBatchId;

    public PipelineSliceReader(PipelineSlice slice, long startBatchId, boolean disposable,
                               PipelineSliceListener listener) {
        this.slice = slice;
        this.listener = listener;
        this.released = new AtomicBoolean();
        this.disposable = disposable;
        this.sliceIterator = disposable ? null : slice.getBufferIterator();
        if (!disposable) {
            totalMessages = slice.getCurrentNumberOfBuffers();
            consumedMessages = 0;
        }
        this.consumedBatchId = -1;
        this.requestBatchId = startBatchId;
    }

    public boolean hasNext() {
        if (isReleased()) {
            throw new IllegalStateException("slice has been released already: " + slice.getSliceId());
        }
        return hasBatch() && (disposable ? slice.hasNext() : consumedMessages < totalMessages);
    }

    public PipeChannelBuffer next() {
        if (!hasBatch()) {
            return null;
        }
        PipeBuffer record = null;
        if (sliceIterator != null && sliceIterator.hasNext()) {
            record = sliceIterator.next();
            if (record != null) {
                consumedMessages++;
            }
        } else {
            record = slice.next();
        }

        if (record == null) {
            return null;
        }
        if (!record.isData()) {
            consumedBatchId = record.getBatchId();
        }
        return new PipeChannelBuffer(record, hasNext());
    }

    public void updateRequestedBatchId(long batchId) {
        this.requestBatchId = batchId;
    }

    public void notifyAvailable(long batchId) {
        if (requestBatchId == -1 || batchId <= requestBatchId) {
            listener.notifyDataAvailable();
        }
    }

    private boolean hasBatch() {
        return requestBatchId == -1 || consumedBatchId < requestBatchId;
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            if (slice.disposeIfNotNeed()) {
                slice.release();
            }
        }
    }

    public boolean isReleased() {
        return released.get() || slice.isReleased();
    }

    public boolean isDisposable() {
        return disposable;
    }

}
