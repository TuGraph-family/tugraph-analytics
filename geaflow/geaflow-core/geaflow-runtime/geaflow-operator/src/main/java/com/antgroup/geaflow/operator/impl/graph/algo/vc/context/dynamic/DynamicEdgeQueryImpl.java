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

package com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic;


import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.DynamicEdgeState;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import java.util.List;

public class DynamicEdgeQueryImpl<K, VV, EV> implements EdgeQuery<K, EV> {

    protected K vId;
    private long versionId;
    private GraphState<K, VV, EV> graphState;

    public DynamicEdgeQueryImpl(K vId, long versionId, GraphState<K, VV, EV> graphState) {
        this.vId = vId;
        this.versionId = versionId;
        this.graphState = graphState;
    }

    @Override
    public List<IEdge<K, EV>> getEdges() {
        DynamicEdgeState<K, VV, EV> edgeState = graphState.dynamicGraph().E();
        return edgeState.query(versionId, vId).asList();
    }

    @Override
    public List<IEdge<K, EV>> getOutEdges() {
        DynamicEdgeState<K, VV, EV> edgeState = graphState.dynamicGraph().E();
        return edgeState.query(versionId, vId).by(OutEdgeFilter.instance()).asList();
    }

    @Override
    public List<IEdge<K, EV>> getInEdges() {
        DynamicEdgeState<K, VV, EV> edgeState = graphState.dynamicGraph().E();
        return edgeState.query(versionId, vId).by(InEdgeFilter.instance()).asList();
    }

    @Override
    public List<IEdge<K, EV>> getEdges(IFilter edgeFilter) {
        DynamicEdgeState<K, VV, EV> edgeState = graphState.dynamicGraph().E();
        return edgeState.query(versionId, vId).by(edgeFilter).asList();
    }
}
