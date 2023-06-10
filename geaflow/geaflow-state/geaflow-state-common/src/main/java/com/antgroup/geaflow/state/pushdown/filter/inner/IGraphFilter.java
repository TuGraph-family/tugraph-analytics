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
import com.antgroup.geaflow.state.pushdown.filter.IFilter;

public interface IGraphFilter extends IFilter<Object> {

    /**
     * An edge will be filtered if return is false.
     * @param edge
     */
    boolean filterEdge(IEdge edge);

    /**
     * A Vertex will be filtered if return is false.
     * @param vertex
     */
    boolean filterVertex(IVertex vertex);

    /**
     * A oneDegreeGraph will be filtered if return is false.
     */
    boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph);

    /**
     * If this returns true, the edge scan will terminate.
     */
    boolean dropAllRemaining();

    /**
     * FilterTypes.
     */
    FilterType getFilterType();

    /**
     * Union other filter with AND logic.
     */
    IGraphFilter and(IGraphFilter other);

    /**
     * Union other filter with or logic.
     */
    IGraphFilter or(IGraphFilter other);

    /**
     * Check filter whether contains some type.
     * Check itself If the filter is simple.
     */
    boolean contains(FilterType type);

    /**
     * retrieve target filter.
     * return itself If the filter is simple.
     */
    IGraphFilter retrieve(FilterType type);

    /**
     * clone the filter.
     */
    IGraphFilter clone();
}
