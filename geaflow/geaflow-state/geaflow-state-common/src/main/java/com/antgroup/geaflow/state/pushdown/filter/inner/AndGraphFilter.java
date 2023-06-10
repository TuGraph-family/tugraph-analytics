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
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import java.util.List;

public class AndGraphFilter extends BaseComposeGraphFilter {

    public AndGraphFilter(List<IGraphFilter> childrenFilters) {
        super(childrenFilters);
    }

    @Override
    public IGraphFilter and(IGraphFilter other) {
        switch (other.getFilterType()) {
            case EMPTY:
                return this;
            case AND:
                childrenFilters.addAll(((AndGraphFilter)(other)).getFilterList());
                return this;
            default:
                childrenFilters.add(other);
                return this;
        }

    }

    /**
     * An edge will be filtered if return is false.
     * @param edge
     */
    @Override
    public boolean filterEdge(IEdge edge) {
        for (IGraphFilter filter: childrenFilters) {
            if (!filter.filterEdge(edge)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A Vertex will be filtered if return is false.
     * @param vertex
     */
    @Override
    public boolean filterVertex(IVertex vertex) {
        for (IGraphFilter filter: childrenFilters) {
            if (!filter.filterVertex(vertex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A oneDegreeGraph will be filtered if return is false.
     */
    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        for (IGraphFilter filter: childrenFilters) {
            if (!filter.filterOneDegreeGraph(oneDegreeGraph)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public AndGraphFilter clone() {
        return new AndGraphFilter(cloneFilterList());
    }

    @Override
    public boolean contains(FilterType type) {
        if (type == FilterType.AND) {
            return true;
        }
        for (IGraphFilter filter: childrenFilters) {
            if (filter.contains(type)) {
                return true;
            }
        }
        return false;

    }

    @Override
    public IGraphFilter retrieve(FilterType type) {
        if (type == FilterType.AND) {
            return this;
        }
        for (IGraphFilter filter: childrenFilters) {
            if (filter.contains(type)) {
                return filter.retrieve(type);
            }
        }
        return null;
    }

    /**
     * FilterTypes.
     */
    @Override
    public FilterType getFilterType() {
        return FilterType.AND;
    }
}
