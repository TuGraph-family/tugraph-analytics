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


import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.VertexQuery;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;

public class DynamicVertexQueryImpl<K, VV, EV> implements VertexQuery<K,VV> {

    private K vertexId;
    private long versionId;
    protected GraphState<K, VV, EV> graphState;

    public DynamicVertexQueryImpl(K vertexId, long versionId, GraphState<K, VV, EV> graphState) {
        this.vertexId = vertexId;
        this.versionId = versionId;
        this.graphState = graphState;
    }

    @Override
    public VertexQuery<K, VV> withId(K vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    @Override
    public IVertex<K, VV> get() {
        return graphState.dynamicGraph().V().query(versionId, vertexId).get();
    }

    @Override
    public IVertex<K, VV> get(IFilter vertexFilter) {
        return graphState.dynamicGraph().V().query(versionId, vertexId).by(vertexFilter).get();
    }
}
