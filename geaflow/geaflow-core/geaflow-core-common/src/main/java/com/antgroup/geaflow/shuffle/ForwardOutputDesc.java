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
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.shuffle.desc.IOutputDesc;
import com.antgroup.geaflow.shuffle.desc.OutputType;
import java.io.Serializable;
import java.util.List;

public class ForwardOutputDesc<T> implements IOutputDesc, Serializable {

    // Execution vertex id.
    private final int vertexId;
    // The current output edge id.
    private final int edgeId;
    // Partition number.
    private final int numPartitions;
    // Data ref count, for data disposal.
    private final int refCount;
    // Name of the output edge.
    private final String edgeName;
    // Data exchange mode.
    private final DataExchangeMode dataExchangeMode;
    // Describe the target task ids which the current output will send data to.
    private final List<Integer> targetTaskIndices;
    // The partitioner of the output data.
    private final IPartitioner<T> partitioner;
    // Data encoder, for serialization and deserialization.
    private final IEncoder<T> encoder;

    public ForwardOutputDesc(
        int vertexId,
        int edgeId,
        int numPartitions,
        int refCount,
        String edgeName,
        DataExchangeMode dataExchangeMode,
        List<Integer> targetTaskIndices,
        IPartitioner<T> partitioner,
        IEncoder<T> encoder) {
        this.vertexId = vertexId;
        this.edgeId = edgeId;
        this.numPartitions = numPartitions;
        this.refCount = refCount;
        this.edgeName = edgeName;
        this.dataExchangeMode = dataExchangeMode;
        this.targetTaskIndices = targetTaskIndices;
        this.partitioner = partitioner;
        this.encoder = encoder;
    }

    public int getVertexId() {
        return this.vertexId;
    }

    public int getEdgeId() {
        return this.edgeId;
    }

    public int getNumPartitions() {
        return this.numPartitions;
    }

    public int getRefCount() {
        return this.refCount;
    }

    public String getEdgeName() {
        return this.edgeName;
    }

    public DataExchangeMode getDataExchangeMode() {
        return this.dataExchangeMode;
    }

    public List<Integer> getTargetTaskIndices() {
        return this.targetTaskIndices;
    }

    public IPartitioner<T> getPartitioner() {
        return this.partitioner;
    }

    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    @Override
    public OutputType getType() {
        return OutputType.FORWARD;
    }

}
