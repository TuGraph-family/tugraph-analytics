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

package com.antgroup.geaflow.dsl.runtime.function.graph;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import java.util.Collections;
import java.util.List;

public class TraversalFromVertexFunction implements MatchVirtualEdgeFunction {

    private final int vertexFieldIndex;
    private final IType<?> vertexFieldType;

    public TraversalFromVertexFunction(int vertexFieldIndex, IType<?> vertexFieldType) {
        this.vertexFieldIndex = vertexFieldIndex;
        this.vertexFieldType = vertexFieldType;
    }

    @Override
    public List<Object> computeTargetId(Path path) {
        Row node = path.getField(vertexFieldIndex, vertexFieldType);
        RowVertex vertex = (RowVertex) node;
        return Collections.singletonList(vertex.getId());
    }

    @Override
    public ITreePath computeTargetPath(Object targetId, ITreePath currentPath) {
        // just traversal from the vertex and cannot carry the paths.
        return null;
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.isEmpty();
        return new TraversalFromVertexFunction(vertexFieldIndex, vertexFieldType);
    }
}
