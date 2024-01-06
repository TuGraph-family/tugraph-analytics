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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RWStaticGraphAccessor<K, VV, EV> extends BaseActionAccess implements IStaticGraphAccessor<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RWStaticGraphAccessor.class);
    private IGraphStore<K, VV, EV> graphStore;

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.graphStore = (IGraphStore<K, VV, EV>) storeBuilder.getStore(DataModel.STATIC_GRAPH, context.getConfig());

        GraphStateDescriptor<K, VV, EV> desc = (GraphStateDescriptor<K, VV, EV>) context.getDescriptor();

        StoreContext storeContext = new StoreContext(context.getName())
            .withConfig(context.getConfig())
            .withMetricGroup(context.getMetricGroup())
            .withDataSchema(desc.getGraphSchema())
            .withName(context.getName())
            .withShardId(context.getShardId());
        this.graphStore.init(storeContext);
        initAction(this.graphStore, context);
    }

    @Override
    public IGraphStore<K, VV, EV> getStore() {
        return this.graphStore;
    }

    protected List<ActionType> allowActionTypes() {
        return Stream.of(ActionType.values()).collect(Collectors.toList());
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        getStore().addEdge(edge);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        return getStore().getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        return getStore().getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return getStore().vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return getStore().vertexIDIterator(pushDown);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        getStore().addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return getStore().getVertex(sid, pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return getStore().getVertexIterator(pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return getStore().getVertexIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return getStore().getEdgeIterator(pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return getStore().getEdgeIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return getStore().getOneDegreeGraphIterator(pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return getStore().getOneDegreeGraphIterator(keys, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getStore().getEdgeProjectIterator(pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys, IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getStore().getEdgeProjectIterator(keys, pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        return getStore().getAggResult(pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        return getStore().getAggResult(keys, pushdown);
    }
}
