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

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseGraphFilter implements IGraphFilter {

    @Override
    public boolean filter(Object value) {
        switch (dateType()) {
            case V:
                return filterVertex((IVertex) value);
            case E:
                return filterEdge((IEdge) value);
            case VE:
                return filterOneDegreeGraph((OneDegreeGraph) value);
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("not support " + dateType()));
        }
    }

    /**
     * An edge will be filtered if return is false.
     * @param edge
     */
    @Override
    public boolean filterEdge(IEdge edge) {
        return true;
    }

    /**
     * A Vertex will be filtered if return is false.
     * @param vertex
     */
    @Override
    public boolean filterVertex(IVertex vertex) {
        return true;
    }

    /**
     * A oneDegreeGraph will be filtered if return is false.
     */
    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        return true;
    }

    /**
     * If this returns true, the edge scan will terminate.
     */
    @Override
    public boolean dropAllRemaining() {
        return false;
    }

    /**
     * FilterTypes.
     */
    @Override
    public FilterType getFilterType() {
        return FilterType.OTHER;
    }

    /**
     * Union other filter with AND logic.
     */
    @Override
    public IGraphFilter and(IGraphFilter other) {
        List<IGraphFilter> list = new ArrayList<>();
        list.add(this);
        if (other.getFilterType() == FilterType.AND) {
            list.addAll(((AndGraphFilter)other).getFilterList());
        } else {
            list.add(other);
        }
        return new AndGraphFilter(list);
    }

    /**
     * Union other filter with OR logic.
     */
    @Override
    public IGraphFilter or(IGraphFilter other) {
        List<IGraphFilter> list = new ArrayList<>();
        list.add(this);
        if (other.getFilterType() == FilterType.OR) {
            list.addAll(((OrGraphFilter)other).getFilterList());
        } else {
            list.add(other);
        }
        return new OrGraphFilter(list);
    }

    @Override
    public boolean contains(FilterType type) {
        return type == getFilterType();
    }

    @Override
    public IGraphFilter retrieve(FilterType type) {
        return contains(type) ? this : null;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }

    @Override
    public IGraphFilter clone() {
        return this;
    }
}
