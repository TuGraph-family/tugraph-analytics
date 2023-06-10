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
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.HistoricalGraph;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.pushdown.filter.IVertexFilter;
import java.util.List;
import java.util.Map;

public class IncHistoricalGraph<K, VV, EV> implements HistoricalGraph<K, VV, EV> {

    protected K vertexId;
    protected final GraphState<K, VV, EV> graphState;

    public IncHistoricalGraph(GraphState<K, VV, EV> graphState) {
        this.graphState = graphState;
    }

    public void init(K vertexId) {
        this.vertexId = vertexId;
    }

    @Override
    public Long getLatestVersionId() {
        return graphState.dynamicGraph().V().getLatestVersion(this.vertexId);
    }

    @Override
    public List<Long> getAllVersionIds() {
        return graphState.dynamicGraph().V().getAllVersions(this.vertexId);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex() {
        return graphState.dynamicGraph().V().query(vertexId).asMap();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions) {
        return graphState.dynamicGraph().V().query(vertexId, versions).asMap();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions,
                                                  IVertexFilter<K, VV> vertexFilter) {
        return graphState.dynamicGraph().V().query(vertexId, versions).by(vertexFilter).asMap();
    }

    @Override
    public GraphSnapShot<K, VV, EV> getSnapShot(long version) {
        return new IncGraphSnapShot(vertexId, version, graphState);
    }
}
