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
import com.antgroup.geaflow.shuffle.util.SliceNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceManager.class);
    private final Map<Long, Set<SliceId>> pipeline2slices = new HashMap<>();
    private final Map<SliceId, IPipelineSlice> slices = new ConcurrentHashMap<>();

    public void register(SliceId sliceId, IPipelineSlice slice) {
        if (this.slices.containsKey(sliceId)) {
            throw new GeaflowRuntimeException("slice already registered: " + sliceId);
        }
        LOGGER.info("register slice {} {}", sliceId, slice.getClass().getSimpleName());
        this.slices.put(sliceId, slice);
        synchronized (this.pipeline2slices) {
            long pipelineId = sliceId.getWriterId().getPipelineId();
            Set<SliceId> sliceIds = this.pipeline2slices.computeIfAbsent(pipelineId, k -> new HashSet<>());
            sliceIds.add(sliceId);
        }
    }

    public IPipelineSlice getSlice(SliceId sliceId) {
        return this.slices.get(sliceId);
    }

    public PipelineSliceReader createSliceReader(SliceId sliceId,
                                                 long startBatchId,
                                                 PipelineSliceListener listener) throws IOException {
        IPipelineSlice slice = this.getSlice(sliceId);
        if (slice == null) {
            throw new SliceNotFoundException(sliceId);
        }
        return slice.createSliceReader(startBatchId, listener);
    }

    public void release(SliceId sliceId) {
        IPipelineSlice slice = this.slices.remove(sliceId);
        if (slice != null && !slice.isReleased()) {
            slice.release();
            LOGGER.info("release slice {}", sliceId);
        }
    }

    public void release(long pipelineId) {
        if (!this.pipeline2slices.containsKey(pipelineId)) {
            return;
        }
        synchronized (this.pipeline2slices) {
            Set<SliceId> sliceIds = this.pipeline2slices.remove(pipelineId);
            if (sliceIds != null) {
                for (SliceId sliceId : sliceIds) {
                    this.release(sliceId);
                }
            }
        }
    }

}
