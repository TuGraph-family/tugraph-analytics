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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.path.EmptyTreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath.PathFilterFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath.PathMapFunction;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class VertexRecord implements StepRecordWithPath {

    private final RowVertex vertex;

    private final ITreePath treePath;

    private VertexRecord(RowVertex vertex, ITreePath treePath) {
        this.vertex = vertex;
        this.treePath = treePath == null ? EmptyTreePath.INSTANCE : treePath;
    }

    public static VertexRecord of(RowVertex vertex, ITreePath treePath) {
        return new VertexRecord(vertex, treePath);
    }

    public RowVertex getVertex() {
        return vertex;
    }

    public ITreePath getTreePath() {
        return treePath;
    }

    @Override
    public StepRecordType getType() {
        return StepRecordType.VERTEX;
    }

    @Override
    public ITreePath getPathById(Object vertexId) {
        if (Objects.equals(vertexId, vertex.getId())) {
            return treePath;
        }
        return null;
    }

    @Override
    public Iterable<ITreePath> getPaths() {
        return Collections.singletonList(treePath);
    }

    @Override
    public Iterable<Object> getVertexIds() {
        return Collections.singletonList(vertex.getId());
    }

    @Override
    public StepRecordWithPath filter(PathFilterFunction function, int[] refPathIndices) {
        ITreePath filterTreePath = treePath.filter(function, refPathIndices);
        return new VertexRecord(vertex, filterTreePath);
    }

    @Override
    public StepRecordWithPath mapPath(PathMapFunction<Path> function, int[] refPathIndices) {
        ITreePath mapTreePath = treePath.mapTree(function);
        return new VertexRecord(vertex, mapTreePath);
    }

    @Override
    public StepRecordWithPath mapTreePath(Function<ITreePath, ITreePath> function) {
        ITreePath mapTreePath = function.apply(treePath);
        return new VertexRecord(vertex, mapTreePath);
    }

    @Override
    public <O> List<O> map(PathMapFunction<O> function, int[] refPathIndices) {
        return treePath.map(function);
    }

    @Override
    public StepRecordWithPath subPathSet(int[] pathIndices) {
        ITreePath subTreePath = treePath.subPath(pathIndices);
        return new VertexRecord(vertex, subTreePath);
    }

    @Override
    public boolean isPathEmpty() {
        return treePath.isEmpty();
    }
}
