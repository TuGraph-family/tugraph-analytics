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

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import java.util.Iterator;

/**
 * RepeatableReader reads the buffer by iterator and remains the slice data unchanged.
 */
public class RepeatableSliceReader extends PipelineSliceReader {
    private final Iterator<PipeBuffer> sliceIterator;
    private final int totalMessages;
    private int consumedMessages;

    public RepeatableSliceReader(IPipelineSlice slice, long startBatchId,
                                 PipelineSliceListener listener) {
        super(slice, startBatchId, listener);
        this.sliceIterator = slice.getBufferIterator();
        this.totalMessages = slice.getTotalBufferCount();
        this.consumedMessages = 0;
    }

    @Override
    public boolean hasNext() {
        if (isReleased()) {
            throw new IllegalStateException("slice has been released already: " + slice.getSliceId());
        }
        return hasBatch() && consumedMessages < totalMessages;
    }

    public PipeChannelBuffer next() {
        PipeBuffer record = null;
        if (sliceIterator.hasNext()) {
            record = sliceIterator.next();
            if (record != null) {
                consumedMessages++;
            }
        }

        if (record == null) {
            return null;
        }
        if (!record.isData()) {
            consumedBatchId = record.getBatchId();
        }
        return new PipeChannelBuffer(record, hasNext());
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            if (slice.canRelease()) {
                slice.release();
            }
            if (sliceIterator instanceof CloseableIterator) {
                ((CloseableIterator) sliceIterator).close();
            }
        }
    }
}
