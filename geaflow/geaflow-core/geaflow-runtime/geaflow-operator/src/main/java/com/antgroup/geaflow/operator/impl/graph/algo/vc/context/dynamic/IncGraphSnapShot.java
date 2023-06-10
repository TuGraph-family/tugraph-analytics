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

import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.GraphSnapShot;
import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.VertexQuery;
import com.antgroup.geaflow.state.GraphState;

public class IncGraphSnapShot<K, VV, EV> implements GraphSnapShot<K, VV, EV> {

    protected final K vertexId;
    protected final long versionId;
    protected final GraphState<K, VV, EV> graphState;

    public IncGraphSnapShot(K vertexId, long versionId, GraphState<K, VV, EV> graphState) {
        this.vertexId = vertexId;
        this.versionId = versionId;
        this.graphState = graphState;
    }

    @Override
    public long getVersion() {
        return versionId;
    }

    @Override
    public VertexQuery<K, VV> vertex() {
        return new DynamicVertexQueryImpl<>(vertexId, versionId, graphState);
    }

    @Override
    public EdgeQuery<K, EV> edges() {
        return new DynamicEdgeQueryImpl<>(vertexId, versionId, graphState);
    }

}
