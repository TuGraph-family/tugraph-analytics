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

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.HistoricalGraph;
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.IncGraphContext;
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.MutableGraph;
import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.TemporaryGraph;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.operator.Operator.OpContext;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import com.antgroup.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;
import com.antgroup.geaflow.state.GraphState;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncGraphContextImpl<K, VV, EV, M> implements IncGraphContext<K, VV, EV, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphContextImpl.class);

    private long iterationId;
    private K vertexId;
    private final OpContext opContext;
    private final RuntimeContext runtimeContext;

    private final IncHistoricalGraph<K, VV, EV> historicalGraph;
    private final IncTemporaryGraph<K, VV, EV> temporaryGraph;
    private final IncMutableGraph<K, VV, EV> mutableGraph;
    private final GraphState<K, VV, EV> graphState;
    private final IGraphMsgBox<K, M> graphMsgBox;
    private final long maxIteration;

    public IncGraphContextImpl(OpContext opContext,
                               RuntimeContext runtimeContext,
                               GraphState<K, VV, EV> graphState,
                               TemporaryGraphCache<K, VV, EV> temporaryGraphCache,
                               IGraphMsgBox<K, M> graphMsgBox,
                               long maxIteration) {
        this.opContext = opContext;
        this.runtimeContext = runtimeContext;
        this.historicalGraph = new IncHistoricalGraph<>(graphState);
        this.temporaryGraph = new IncTemporaryGraph<>(temporaryGraphCache);
        this.mutableGraph = new IncMutableGraph<>(graphState);
        this.graphMsgBox = graphMsgBox;
        this.graphState = graphState;
        this.maxIteration = maxIteration;
    }

    public void init(long iterationId, K vertexId) {
        this.iterationId = iterationId;
        this.vertexId = vertexId;

        this.historicalGraph.init(vertexId);
        this.temporaryGraph.init(vertexId);

    }

    @Override
    public long getJobId() {
        return opContext.getRuntimeContext().getPipelineId();
    }

    @Override
    public long getIterationId() {
        return iterationId;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return this.runtimeContext;
    }

    @Override
    public MutableGraph<K, VV, EV> getMutableGraph() {
        return this.mutableGraph;
    }

    @Override
    public TemporaryGraph<K, VV, EV> getTemporaryGraph() {
        return this.temporaryGraph;
    }

    @Override
    public HistoricalGraph<K, VV, EV> getHistoricalGraph() {
        return this.historicalGraph;
    }

    @Override
    public void sendMessage(K vertexId, M m) {
        if (this.iterationId >= this.maxIteration) {
            return;
        }
        graphMsgBox.addOutMessage(vertexId, m);
    }

    @Override
    public void sendMessageToNeighbors(M m) {
        if (this.iterationId >= this.maxIteration) {
            return;
        }
        List<Long> allVersions = graphState.dynamicGraph().V().getAllVersions(vertexId);
        for (long version: allVersions) {
            Iterator<IEdge<K, EV>> edgeIterator =
                graphState.dynamicGraph().E().query(version, vertexId).iterator();
            while (edgeIterator.hasNext()) {
                IEdge<K, EV> edge = edgeIterator.next();
                graphMsgBox.addOutMessage(edge.getTargetId(), m);
            }
        }
    }

}
