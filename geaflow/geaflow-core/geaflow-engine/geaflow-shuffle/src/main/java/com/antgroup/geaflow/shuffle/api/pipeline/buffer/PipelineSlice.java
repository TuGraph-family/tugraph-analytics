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

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceListener;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceReader;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineSlice.class);

    private final SliceId sliceId;
    private final String taskLogTag;
    private final ArrayDeque<PipeBuffer> buffers;
    private PipelineSliceReader sliceReader;
    // Reference count of the slice.
    private int refCount;
    // Flag indicating whether the slice has been released.
    private volatile boolean isReleased;
    // Flag indicating whether flush is requested.
    private boolean flushRequested;

    public PipelineSlice(String taskLogTag, SliceId sliceId) {
        this(taskLogTag, sliceId, 1);
    }

    public PipelineSlice(String taskLogTag, SliceId sliceId, int refCount) {
        this.sliceId = sliceId;
        this.taskLogTag = taskLogTag;
        this.refCount = refCount;
        this.buffers = new ArrayDeque<>();
    }

    // ------------------------------------------------------------------------
    // Produce
    // ------------------------------------------------------------------------

    public boolean add(PipeBuffer recordBuffer) {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (isReleased) {
                return false;
            }
            buffers.add(recordBuffer);
            notifyDataAvailable = shouldNotifyDataAvailable();
        }

        if (notifyDataAvailable) {
            notifyDataAvailable(recordBuffer.getBatchId());
        }
        return true;
    }

    private boolean shouldNotifyDataAvailable() {
        return sliceReader != null && !this.flushRequested && getCurrentNumberOfBuffers() == 1;
    }

    public void flush() {
        long batchId;
        boolean needNotify;
        synchronized (buffers) {
            if (buffers.isEmpty()) {
                return;
            }

            batchId = buffers.peekLast().getBatchId();
            needNotify = !flushRequested && buffers.size() == 1;
            updateFlushRequested(flushRequested || buffers.size() > 1 || needNotify);
        }

        if (needNotify) {
            notifyDataAvailable(batchId);
        }
    }

    private void notifyDataAvailable(long batchId) {
        final PipelineSliceReader reader = sliceReader;
        if (reader != null) {
            reader.notifyAvailable(batchId);
        }
    }

    public void setRefCount(int refCount) {
        synchronized (buffers) {
            this.refCount = refCount;
        }
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    public PipelineSliceReader createSliceReader(long startBatchId, PipelineSliceListener listener) {
        synchronized (buffers) {
            if (isReleased) {
                throw new GeaflowRuntimeException("slice is released:" + sliceId);
            }
            refCount--;
            sliceReader = new PipelineSliceReader(this, startBatchId, refCount < 1, listener);
            LOGGER.info("creating reader for {} {} with startBatch:{} refCount:{} disposable:{}",
                taskLogTag, sliceId, startBatchId, refCount, sliceReader.isDisposable());
        }
        return sliceReader;
    }

    public PipeBuffer next() {
        synchronized (buffers) {
            PipeBuffer buffer = null;
            if (!buffers.isEmpty()) {
                buffer = buffers.pop();
                if (buffers.size() == 0) {
                    updateFlushRequested(false);
                }
            }
            return buffer;
        }
    }

    public boolean hasNext() {
        synchronized (buffers) {
            return this.flushRequested || getCurrentNumberOfBuffers() > 0;
        }
    }

    public int getCurrentNumberOfBuffers() {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        return buffers.size();
    }

    @VisibleForTesting
    public int getNumberOfBuffers() {
        return buffers.size();
    }

    private void updateFlushRequested(boolean flushRequested) {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        this.flushRequested = flushRequested;
    }

    public boolean disposeIfNotNeed() {
        return refCount == 0 && !hasNext();
    }

    public Iterator<PipeBuffer> getBufferIterator() {
        return buffers.iterator();
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    @VisibleForTesting
    public PipelineSliceReader getSliceReader() {
        return sliceReader;
    }

    public boolean isReleased() {
        return isReleased;
    }

    public void release() {
        final PipelineSliceReader reader;
        int bufferSize;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            bufferSize = buffers.size();
            buffers.clear();

            reader = sliceReader;
            sliceReader = null;
            isReleased = true;
        }

        LOGGER.info("{}: released {} with bufferSize:{}", taskLogTag, sliceId, bufferSize);
        if (reader != null) {
            reader.release();
        }

        ShuffleDataManager.getInstance().release(sliceId);
    }

}
