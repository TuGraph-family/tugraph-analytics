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

import com.antgroup.geaflow.shuffle.message.SliceId;

/**
 * Message consumed by channels ({@link com.antgroup.geaflow.shuffle.api.pipeline.channel.LocalInputChannel}
 * and {@link com.antgroup.geaflow.shuffle.api.pipeline.fetcher.SequenceSliceReader}).
 */
public class PipeChannelBuffer {

    private final PipeBuffer buffer;
    // Indicate the availability of message in PipeSlice.
    private final boolean moreAvailable;
    // Input slice id.
    private SliceId sliceId;

    public PipeChannelBuffer(PipeBuffer buffer, boolean moreAvailable) {
        this.buffer = buffer;
        this.moreAvailable = moreAvailable;
    }

    public PipeChannelBuffer(PipeBuffer buffer, boolean moreAvailable, SliceId sliceId) {
        this.buffer = buffer;
        this.moreAvailable = moreAvailable;
        this.sliceId = sliceId;
    }

    public PipeBuffer getBuffer() {
        return buffer;
    }

    public boolean moreAvailable() {
        return moreAvailable;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public void setSliceId(SliceId sliceId) {
        this.sliceId = sliceId;
    }

}
