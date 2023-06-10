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

import java.util.concurrent.atomic.AtomicInteger;

public class PipelineShard {

    private final PipelineSlice[] pipeSlices;
    private final AtomicInteger sliceNum;
    private final transient String taskName;

    public PipelineShard(PipelineSlice[] slices) {
        this.taskName = null;
        this.pipeSlices = slices;
        this.sliceNum = new AtomicInteger(slices.length);
    }

    public PipelineShard(String taskName, PipelineSlice[] slices) {
        this.taskName = taskName;
        this.pipeSlices = slices;
        this.sliceNum = new AtomicInteger(slices.length);
    }

    public PipelineShard(String taskName, PipelineSlice[] slices, int sliceNum) {
        this.taskName = taskName;
        this.pipeSlices = slices;
        this.sliceNum = new AtomicInteger(sliceNum);
    }

    public PipelineSlice getSlice(int sliceIndex) {
        return pipeSlices[sliceIndex];
    }

    public PipelineSlice[] getSlices() {
        return pipeSlices;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getSliceNum() {
        return pipeSlices.length;
    }

    public int getBufferCount() {
        int bufferCount = 0;
        for (int i = 0; i < pipeSlices.length; i++) {
            PipelineSlice slice = pipeSlices[i];
            if (slice != null) {
                bufferCount += slice.getNumberOfBuffers();
            }
        }
        return bufferCount;
    }

    public boolean hasData() {
        for (PipelineSlice slice : pipeSlices) {
            if (slice.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public void release(int sliceIndex) {
        synchronized (this) {
            PipelineSlice slice = pipeSlices[sliceIndex];
            if (slice != null) {
                slice.release();
                pipeSlices[sliceIndex] = null;
                sliceNum.decrementAndGet();
            }
        }
    }

    public void release() {
        synchronized (this) {
            for (int i = 0; i < pipeSlices.length; i++) {
                PipelineSlice slice = pipeSlices[i];
                if (slice != null) {
                    slice.release();
                    pipeSlices[i] = null;
                }
            }
            sliceNum.set(0);
        }
    }

    public boolean disposedIfNeed() {
        return sliceNum.get() <= 0;
    }
}
