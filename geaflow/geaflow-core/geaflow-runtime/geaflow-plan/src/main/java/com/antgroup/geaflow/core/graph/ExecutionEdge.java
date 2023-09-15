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

package com.antgroup.geaflow.core.graph;

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.partitioner.IPartitioner;
import java.io.Serializable;

public class ExecutionEdge implements Serializable {

    private IPartitioner partitioner;
    private String edgeName;
    private int edgeId;
    private int srcId;
    private int targetId;
    private IEncoder<?> encoder;
    private CollectType type;

    public ExecutionEdge(IPartitioner partitioner,
                         int edgeId,
                         String edgeName,
                         int srcId,
                         int targetId,
                         IEncoder<?> encoder) {
        this(partitioner, edgeId, edgeName, srcId, targetId, CollectType.FORWARD, encoder);
    }

    public ExecutionEdge(IPartitioner partitioner,
                         int edgeId,
                         String edgeName,
                         int srcId,
                         int targetId,
                         CollectType type,
                         IEncoder<?> encoder) {
        this.partitioner = partitioner;
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.srcId = srcId;
        this.targetId = targetId;
        this.encoder = encoder;
        this.type = type;
    }

    public IPartitioner getPartitioner() {
        return partitioner;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(int edgeId) {
        this.edgeId = edgeId;
    }

    public void setPartitioner(IPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public String getEdgeName() {
        return edgeName;
    }

    public void setEdgeName(String edgeName) {
        this.edgeName = edgeName;
    }

    public int getSrcId() {
        return srcId;
    }

    public void setSrcId(int srcId) {
        this.srcId = srcId;
    }

    public int getTargetId() {
        return targetId;
    }

    public void setTargetId(int targetId) {
        this.targetId = targetId;
    }

    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    public void setEncoder(IEncoder<?> encoder) {
        this.encoder = encoder;
    }

    public CollectType getType() {
        return type;
    }

    public void setType(CollectType type) {
        this.type = type;
    }

}
