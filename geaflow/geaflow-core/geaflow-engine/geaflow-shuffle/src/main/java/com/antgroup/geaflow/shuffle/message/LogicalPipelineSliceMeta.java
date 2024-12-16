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

public class LogicalPipelineSliceMeta extends BaseSliceMeta {

    private final SliceId sliceId;
    private final String containerId;

    public LogicalPipelineSliceMeta(int sourceIndex, int targetIndex, long pipelineId, int edgeId,
                                    String containerId) {
        super(sourceIndex, targetIndex);
        this.windowId = -1;
        this.sliceId = new SliceId(pipelineId, edgeId, sourceIndex, targetIndex);
        this.containerId = containerId;
        setEdgeId(edgeId);
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public String getContainerId() {
        return containerId;
    }

}
