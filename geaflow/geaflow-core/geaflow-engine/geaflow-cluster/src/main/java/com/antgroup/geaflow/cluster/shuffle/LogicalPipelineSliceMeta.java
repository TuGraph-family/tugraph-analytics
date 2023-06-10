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

package com.antgroup.geaflow.cluster.shuffle;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.shuffle.message.BaseSliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.SliceId;

public class LogicalPipelineSliceMeta extends BaseSliceMeta {

    private final SliceId sliceId;
    private String containerId;

    public LogicalPipelineSliceMeta(int sourceIndex, int targetIndex, long pipelineId, int edgeId,
                                    String containerId) {
        super(sourceIndex, targetIndex);
        this.batchId = -1;
        this.sliceId = new SliceId(pipelineId, edgeId, sourceIndex, targetIndex);
        this.containerId = containerId;
        setEdgeId(edgeId);
    }

    public LogicalPipelineSliceMeta(SliceId sliceId, long batchId, String containerId) {
        super(sliceId.getShardIndex(), sliceId.getSliceIndex());
        this.batchId = batchId;
        this.sliceId = sliceId;
        this.containerId = containerId;
        setEdgeId(sliceId.getEdgeId());
    }

    public long getBatchId() {
        return batchId;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public String getContainerId() {
        return containerId;
    }

    public PipelineSliceMeta toPhysicalPipelineSliceMeta() {
        ResourceData resourceData = HAServiceFactory.getService().resolveResource(containerId);
        return new PipelineSliceMeta(
            sliceId, batchId,
            new ShuffleAddress(resourceData.getHost(), resourceData.getShufflePort()));
    }

}
