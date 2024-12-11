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

import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineSlice extends AbstractSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineSlice.class);

    private boolean flushRequested;

    public PipelineSlice(String taskLogTag, SliceId sliceId) {
        super(taskLogTag, sliceId, 1);
    }

    public PipelineSlice(String taskLogTag, SliceId sliceId, int refCount) {
        super(taskLogTag, sliceId, refCount);
    }

    // ------------------------------------------------------------------------
    // Produce
    // ------------------------------------------------------------------------

    @Override
    public boolean add(PipeBuffer recordBuffer) {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (isReleased) {
                return false;
            }
            totalBufferCount++;
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

    @Override
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

    @Override
    public boolean isReady2read() {
        return true;
    }

    @Override
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

    @Override
    public boolean hasNext() {
        synchronized (buffers) {
            return this.flushRequested || getCurrentNumberOfBuffers() > 0;
        }
    }

    private int getCurrentNumberOfBuffers() {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        return buffers.size();
    }

    private void updateFlushRequested(boolean flushRequested) {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        this.flushRequested = flushRequested;
    }

    @Override
    public Iterator<PipeBuffer> getBufferIterator() {
        return buffers.iterator();
    }

    @Override
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
    }

}
