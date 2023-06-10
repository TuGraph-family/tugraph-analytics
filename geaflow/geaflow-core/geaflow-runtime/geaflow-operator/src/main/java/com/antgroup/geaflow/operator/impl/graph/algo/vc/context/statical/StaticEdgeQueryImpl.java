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

package com.antgroup.geaflow.operator.impl.graph.algo.vc.context.statical;


import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import java.util.List;

public class StaticEdgeQueryImpl<K, VV, EV> implements EdgeQuery<K, EV> {

    protected K vId;
    private final GraphState<K, VV, EV> graphState;

    public StaticEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState) {
        this.vId = vId;
        this.graphState = graphState;
    }

    @Override
    public List<IEdge<K, EV>> getEdges() {
        return graphState.staticGraph().E().query(vId).asList();
    }

    @Override
    public List<IEdge<K, EV>> getOutEdges() {
        return graphState.staticGraph().E().query(vId).by(OutEdgeFilter.instance()).asList();
    }

    @Override
    public List<IEdge<K, EV>> getInEdges() {
        return graphState.staticGraph().E().query(vId).by(InEdgeFilter.instance()).asList();
    }

    @Override
    public List<IEdge<K, EV>> getEdges(IFilter edgeFilter) {
        return graphState.staticGraph().E().query(vId).by(edgeFilter).asList();
    }
}
