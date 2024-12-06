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

package com.antgroup.geaflow.shuffle.api.writer;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.SpillablePipelineSlice;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.Shard;
import com.antgroup.geaflow.shuffle.message.SliceId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SpillableShardBuffer<T> extends ShardBuffer<T, Shard> {

    protected final ShuffleAddress shuffleAddress;

    public SpillableShardBuffer(ShuffleAddress shuffleAddress) {
        this.shuffleAddress = shuffleAddress;
    }

    @Override
    protected PipelineSlice newSlice(String taskLogTag, SliceId sliceId, int refCount) {
        return new SpillablePipelineSlice(taskLogTag, sliceId, refCount);
    }

    @Override
    public Optional<Shard> doFinish(long windowId) {
        List<ISliceMeta> slices = this.buildSliceMeta(windowId);
        return Optional.of(new Shard(this.edgeId, slices));
    }

    private List<ISliceMeta> buildSliceMeta(long windowId) {
        List<ISliceMeta> slices = new ArrayList<>();
        for (int i = 0; i < this.targetChannels; i++) {
            SliceId sliceId = this.resultSlices[i].getSliceId();
            PipelineSliceMeta sliceMeta = new PipelineSliceMeta(sliceId, windowId, this.shuffleAddress);
            sliceMeta.setRecordNum(this.recordCounter[i]);
            sliceMeta.setEncodedSize(this.bytesCounter[i]);
            slices.add(sliceMeta);
        }
        return slices;
    }

}
