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
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalHistoricalGraph;
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.HistoricalGraph;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.pushdown.filter.IVertexFilter;
import java.util.List;
import java.util.Map;

public class TraversalIncHistoricalGraph<K, VV, EV> implements HistoricalGraph<K, VV, EV>
     ,TraversalHistoricalGraph<K, VV, EV> {

    private final IncHistoricalGraph<K, VV, EV> historicalGraph;

    public TraversalIncHistoricalGraph(IncHistoricalGraph<K, VV, EV> historicalGraph) {
        this.historicalGraph = historicalGraph;
    }

    @Override
    public Long getLatestVersionId() {
        return historicalGraph.getLatestVersionId();
    }

    @Override
    public List<Long> getAllVersionIds() {
        return historicalGraph.getAllVersionIds();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex() {
        return historicalGraph.getAllVertex();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions) {
        return historicalGraph.getAllVertex(versions);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions, IVertexFilter<K, VV> vertexFilter) {
        return historicalGraph.getAllVertex(versions, vertexFilter);
    }

    @Override
    public TraversalGraphSnapShot<K, VV, EV> getSnapShot(long version) {
        return new TraversalIncGraphSnapShot<>(historicalGraph.vertexId, version, historicalGraph.graphState);
    }
}
