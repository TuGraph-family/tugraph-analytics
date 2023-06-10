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

import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import com.antgroup.geaflow.state.GraphState;

public class TraversalIncGraphSnapShot<K, VV, EV> extends IncGraphSnapShot<K, VV, EV>
    implements TraversalGraphSnapShot<K, VV, EV> {

    public TraversalIncGraphSnapShot(K vertexId, long versionId,
                                     GraphState<K, VV, EV> graphState) {
        super(vertexId, versionId, graphState);
    }

    @Override
    public TraversalVertexQuery<K, VV> vertex() {
        return new DynamicTraversalVertexQueryImpl<>(vertexId, versionId, graphState);
    }

    @Override
    public TraversalEdgeQuery<K, EV> edges() {
        return new DynamicTraversalEdgeQueryImpl<>(vertexId, versionId, graphState);
    }
}
