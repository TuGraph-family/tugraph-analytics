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

package com.antgroup.geaflow.state.pushdown.filter.inner;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;

public class EmptyGraphFilter extends BaseGraphFilter {

    private static final EmptyGraphFilter filter = new EmptyGraphFilter();

    public static EmptyGraphFilter of() {
        return filter;
    }

    @Override
    public IGraphFilter and(IGraphFilter other) {
        return other;
    }

    @Override
    public IGraphFilter or(IGraphFilter other) {
        return other;
    }

    @Override
    public boolean filterEdge(IEdge edge) {
        return true;
    }

    @Override
    public boolean filterVertex(IVertex vertex) {
        return true;
    }

    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        return true;
    }

    @Override
    public DataType dateType() {
        return DataType.OTHER;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.EMPTY;
    }
}
