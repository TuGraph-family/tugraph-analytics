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

public interface IPipelineSlice {

    SliceId getSliceId();

    int getTotalBufferCount();

    boolean isReleased();

    boolean canRelease();

    void release();

    // ------------------------------------------------------------------------
    // Produce
    // ------------------------------------------------------------------------

    boolean add(PipeBuffer recordBuffer);

    void flush();

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    PipelineSliceReader createSliceReader(long startBatchId, PipelineSliceListener listener);

    /**
     * Check whether the slice is ready to read.
     */
    boolean isReady2read();

    /**
     * Check whether the slice has next record.
     * */
    boolean hasNext();

    /**
     * Poll next record from slice.
     * @return
     */
    PipeBuffer next();

}
