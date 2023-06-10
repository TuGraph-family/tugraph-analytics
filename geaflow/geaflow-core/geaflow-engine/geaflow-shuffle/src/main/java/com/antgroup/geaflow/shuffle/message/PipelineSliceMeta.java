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

package com.antgroup.geaflow.shuffle.message;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;

public class PipelineSliceMeta extends BaseSliceMeta {

    private final SliceId sliceId;
    private final ShuffleAddress shuffleAddress;

    public PipelineSliceMeta(int sourceIndex, int targetIndex, long pipelineId, int edgeId,
                             ShuffleAddress address) {
        super(sourceIndex, targetIndex);
        this.batchId = -1;
        this.sliceId = new SliceId(pipelineId, edgeId, sourceIndex, targetIndex);
        this.shuffleAddress = address;
        setEdgeId(edgeId);
    }

    public PipelineSliceMeta(SliceId sliceId, long batchId, ShuffleAddress address) {
        super(sliceId.getShardIndex(), sliceId.getSliceIndex());
        this.batchId = batchId;
        this.sliceId = sliceId;
        this.shuffleAddress = address;
        setEdgeId(sliceId.getEdgeId());
    }

    public long getBatchId() {
        return batchId;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public ShuffleAddress getShuffleAddress() {
        return shuffleAddress;
    }
}
