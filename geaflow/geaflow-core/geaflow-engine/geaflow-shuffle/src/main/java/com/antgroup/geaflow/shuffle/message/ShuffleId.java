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

import java.io.Serializable;

public class ShuffleId implements Serializable {

    private String pipelineName;
    private int vertexId;
    private int outEdgeId;
    private long batchId;

    public ShuffleId() {
    }

    public ShuffleId(String pipelineName, int vertexId, int outEdgeId) {
        this(pipelineName, vertexId, outEdgeId, 0);
    }

    public ShuffleId(String pipelineName, int vertexId, int outEdgeId, long batchId) {
        this.pipelineName = pipelineName;
        this.vertexId = vertexId;
        this.outEdgeId = outEdgeId;
        this.batchId = batchId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public int getOutEdgeId() {
        return outEdgeId;
    }

    public void setOutEdgeId(int outEdgeId) {
        this.outEdgeId = outEdgeId;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    @Override
    public String toString() {
        if (batchId == 0) {
            return String.format("%s-%s-%s", pipelineName, vertexId, outEdgeId);
        } else {
            return String.format("%s-%s-%s-%s", pipelineName, vertexId, outEdgeId, batchId);
        }
    }

}
