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

package com.antgroup.geaflow.shuffle.pipeline.buffer;

public abstract class AbstractBuffer implements OutBuffer {

    private final boolean memoryTrack;
    protected int refCount;

    public AbstractBuffer(boolean memoryTrack) {
        this.memoryTrack = memoryTrack;
    }

    @Override
    public void setRefCount(int refCount) {
        this.refCount = refCount;
    }

    @Override
    public boolean isDisposable() {
        return this.refCount <= 0;
    }

    @Override
    public boolean isMemoryTracking() {
        return this.memoryTrack;
    }

    protected void requireMemory(long dataSize) {
        if (this.memoryTrack) {
            ShuffleMemoryTracker.getInstance().requireMemory(dataSize);
        }
    }

    protected void releaseMemory(long dataSize) {
        if (this.memoryTrack) {
            ShuffleMemoryTracker.getInstance().releaseMemory(dataSize);
        }
    }

    protected abstract static class AbstractBufferBuilder implements BufferBuilder {

        private long recordCount;
        protected boolean memoryTrack;

        @Override
        public long getRecordCount() {
            return this.recordCount;
        }

        @Override
        public void increaseRecordCount() {
            this.recordCount++;
        }

        protected void resetRecordCount() {
            this.recordCount = 0;
        }

        @Override
        public void enableMemoryTrack() {
            this.memoryTrack = true;
        }

    }

}
