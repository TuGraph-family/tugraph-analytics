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

package com.antgroup.geaflow.shuffle;

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.partitioner.IPartitioner;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ForwardOutputDesc implements IOutputDesc, Serializable {

    // Describe the target task ids which the current output will send data to.
    private List<Integer> targetTaskIndices;

    // The current output edge id.
    private int edgeId;

    // Name of the output edge.
    private String edgeName;

    // Execution vertex id.
    private int vertexId;

    // The partitioner of the output data.
    private IPartitioner partitioner;
    private IEncoder<?> encoder;
    private int numPartitions;

    private ShuffleDescriptor shuffleDescriptor;

    public ForwardOutputDesc(int edgeId, String edgeName, int vertexId, ShuffleDescriptor shuffleDescriptor) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.vertexId = vertexId;
        this.shuffleDescriptor = shuffleDescriptor;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public String getEdgeName() {
        return edgeName;
    }

    @Override
    public CollectType getType() {
        return CollectType.FORWARD;
    }

    public int getVertexId() {
        return vertexId;
    }

    public List<Integer> getTargetTaskIndices() {
        return targetTaskIndices;
    }

    public void setTargetTaskIndices(List<Integer> targetTaskIndices) {
        this.targetTaskIndices = targetTaskIndices;
    }

    public IPartitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(IPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    public void setEncoder(IEncoder<?> encoder) {
        this.encoder = encoder;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    public ForwardOutputDesc clone() {
        ForwardOutputDesc outputDesc = new ForwardOutputDesc(this.edgeId,
            this.edgeName, this.vertexId, this.shuffleDescriptor);
        if (this.partitioner != null) {
            outputDesc.setPartitioner(this.partitioner);
        }
        if (this.targetTaskIndices != null) {
            outputDesc.setTargetTaskIndices(new ArrayList<>(this.targetTaskIndices));
        }
        if (this.encoder != null) {
            outputDesc.setEncoder(this.encoder);
        }
        outputDesc.setNumPartitions(this.numPartitions);
        return outputDesc;
    }
}
