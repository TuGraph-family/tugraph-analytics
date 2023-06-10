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

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import com.antgroup.geaflow.state.GraphState;

public class StaticTraversalEdgeQueryImpl<K, VV, EV> extends StaticEdgeQueryImpl<K, VV, EV>
    implements TraversalEdgeQuery<K, EV> {

    public StaticTraversalEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState) {
        super(vId, graphState);
    }

    @Override
    public TraversalEdgeQuery<K, EV> withId(K vertexId) {
        this.vId = vertexId;
        return this;
    }
}
