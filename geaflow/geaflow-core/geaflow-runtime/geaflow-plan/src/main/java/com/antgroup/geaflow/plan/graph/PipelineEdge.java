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

package com.antgroup.geaflow.plan.graph;

import com.antgroup.geaflow.api.partition.kv.RandomPartition;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.partitioner.IPartitioner.PartitionType;
import java.io.Serializable;
import java.util.Objects;

public class PipelineEdge implements Serializable {

    private int edgeId;
    private int srcId;
    private int targetId;
    private int streamOrdinal;
    private IPartitioner partition;
    private PartitionType partitionType;
    private String edgeName;
    private CollectType type;
    private IEncoder<?> encoder;

    public PipelineEdge(int edgeId, int srcId, int targetId, IPartitioner partition, IEncoder<?> encoder) {
        this(edgeId, srcId, targetId, partition, 0, encoder);
    }

    public PipelineEdge(int edgeId, int srcId, int targetId, IPartitioner partition,
                        IEncoder<?> encoder, CollectType type) {
        this(edgeId, srcId, targetId, partition, 0, encoder);
        this.type = type;
    }

    public PipelineEdge(int edgeId, int srcId, int targetId, IPartitioner partition,
                        int streamOrdinal, IEncoder<?> encoder) {
        this.edgeId = edgeId;
        this.srcId = srcId;
        this.targetId = targetId;
        this.partition = partition;
        this.streamOrdinal = streamOrdinal;
        this.partitionType = partition.getPartitionType();
        this.encoder = encoder;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(int edgeId) {
        this.edgeId = edgeId;
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

    public IPartitioner getPartition() {
        return partition;
    }

    public void setPartition(IPartitioner partition) {
        this.partition = partition;
    }

    public int getStreamOrdinal() {
        return streamOrdinal;
    }

    public void setStreamOrdinal(int streamOrdinal) {
        this.streamOrdinal = streamOrdinal;
    }

    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    public void setEncoder(IEncoder<?> encoder) {
        this.encoder = encoder;
    }

    public String getEdgeName() {
        if (edgeName != null) {
            return edgeName;
        }

        String partitionName = "";
        if (this.partition.getPartition() != null && !(this.partition
                .getPartition() instanceof RandomPartition)) {
            partitionName =
                    "-partitionFunc-" + this.partition.getPartition().getClass().getSimpleName();
        }

        if (srcId != targetId) {
            return this.edgeId + "-stream-from" + srcId + "-to" + targetId + partitionName;
        } else {
            return this.edgeId + "-stream-from" + srcId + "-IteratorStream";
        }
    }

    public CollectType getType() {
        return type;
    }

    public void setEdgeName(String edgeName) {
        this.edgeName = edgeName;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.edgeId, this.srcId, this.targetId, edgeName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PipelineEdge) {
            PipelineEdge other = (PipelineEdge) obj;
            return other.getSrcId() == srcId && other.getTargetId() == targetId
                && other.getEdgeId() == edgeId && other.getPartitionType() == partitionType
                && Objects.equals(getEdgeName(), other.getEdgeName());
        }
        return false;
    }

    public enum JoinStream {
        /**
         * left.
         */
        left,
        /**
         * right.
         */
        right,
        /**
         * none.
         */
        none;
    }
}

