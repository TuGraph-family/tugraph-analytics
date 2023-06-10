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

import com.antgroup.geaflow.partitioner.IPartitioner;

import java.io.Serializable;

public class ExecutionVertexGroupEdge implements Serializable {

    private int groupEdgeId;
    private int groupSrcId;
    private int groupTargetId;
    private IPartitioner partitioner;
    private String groupEdgeName;

    public ExecutionVertexGroupEdge(IPartitioner partitioner, int groupEdgeId,
                                    String groupEdgeName, int groupSrcId, int groupTargetId) {
        this.partitioner = partitioner;
        this.groupEdgeId = groupEdgeId;
        this.groupEdgeName = groupEdgeName;
        this.groupSrcId = groupSrcId;
        this.groupTargetId = groupTargetId;
    }

    public int getGroupEdgeId() {
        return groupEdgeId;
    }

    public void setGroupEdgeId(int groupEdgeId) {
        this.groupEdgeId = groupEdgeId;
    }

    public int getGroupSrcId() {
        return groupSrcId;
    }

    public void setGroupSrcId(int groupSrcId) {
        this.groupSrcId = groupSrcId;
    }

    public int getGroupTargetId() {
        return groupTargetId;
    }

    public void setGroupTargetId(int groupTargetId) {
        this.groupTargetId = groupTargetId;
    }

    public IPartitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(IPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public String getGroupEdgeName() {
        return groupEdgeName;
    }

    public void setGroupEdgeName(String groupEdgeName) {
        this.groupEdgeName = groupEdgeName;
    }
}
