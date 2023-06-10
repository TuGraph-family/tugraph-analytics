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


import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.MutableGraph;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.GraphState;

public class IncMutableGraph<K, VV, EV> implements MutableGraph<K, VV, EV> {

    private GraphState<K, VV, EV> graphState;

    public IncMutableGraph(GraphState<K, VV, EV> graphState) {
        this.graphState = graphState;
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        graphState.dynamicGraph().V().add(version, vertex);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        graphState.dynamicGraph().E().add(version, edge);
    }


}
