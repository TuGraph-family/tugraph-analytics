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

package com.antgroup.geaflow.dsl.runtime.traversal.path;

import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SourceEdgeTreePath extends AbstractSingleTreePath {

    /**
     * Edge sets with same (srcId, targetId) pair.
     */
    private final EdgeSet edges;

    private SourceEdgeTreePath(Set<Object> requestIds, EdgeSet edges) {
        this.edges = edges;
        this.requestIds = requestIds;
    }

    public static SourceEdgeTreePath of(Set<Object> requestIds, EdgeSet edges) {
        return new SourceEdgeTreePath(requestIds, edges);
    }

    public static SourceEdgeTreePath of(Object requestId, EdgeSet edges) {
        if (requestId == null) {
            return of(null, edges);
        }
        return of(Sets.newHashSet(requestId), edges);
    }

    @Override
    public RowVertex getVertex() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public void setVertex(RowVertex vertex) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public Object getVertexId() {
        return edges.getTargetId();
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.EDGE_TREE;
    }

    @Override
    public List<ITreePath> getParents() {
        return new ArrayList<>();
    }

    @Override
    public EdgeSet getEdgeSet() {
        return edges;
    }

    @Override
    public ITreePath copy() {
        return new SourceEdgeTreePath(ArrayUtil.copySet(requestIds), edges.copy());
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        assert parents.isEmpty();
        return copy();
    }

    @Override
    public ITreePath getTreePath(Object requestId) {
        if (requestIds != null && !requestIds.contains(requestId)) {
            return null;
        }
        return of(requestId, edges.copy());
    }

    @Override
    public int size() {
        return edges.size();
    }

    @Override
    public boolean equalNode(ITreePath other) {
        if (other.getNodeType() == NodeType.EDGE_TREE) {
            return Objects.equals(edges, other.getEdgeSet());
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceEdgeTreePath)) {
            return false;
        }
        SourceEdgeTreePath that = (SourceEdgeTreePath) o;
        return Objects.equals(edges, that.edges) && Objects.equals(requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges, requestIds);
    }
}
