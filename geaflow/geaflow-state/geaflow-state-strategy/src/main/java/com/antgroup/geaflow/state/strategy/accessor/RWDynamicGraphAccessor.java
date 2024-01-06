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
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.graph.IGraphMultiVersionedStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RWDynamicGraphAccessor<K, VV, EV> extends BaseActionAccess implements DynamicGraphAccessor<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RWDynamicGraphAccessor.class);
    private IGraphMultiVersionedStore<K, VV, EV> graphStore;

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.graphStore = (IGraphMultiVersionedStore<K, VV, EV>) storeBuilder.getStore(
            DataModel.DYNAMIC_GRAPH, context.getConfig());

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
    public IGraphMultiVersionedStore<K, VV, EV> getStore() {
        return graphStore;
    }

    protected List<ActionType> allowActionTypes() {
        return Stream.of(ActionType.values()).collect(Collectors.toList());
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        getStore().addEdge(version, edge);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        return getStore().getEdges(version, sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid,
                                                       IStatePushDown pushdown) {
        return getStore().getOneDegreeGraph(version, sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return getStore().vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown) {
        return getStore().vertexIDIterator(version, pushdown);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        getStore().addVertex(version, vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        return getStore().getVertex(version, sid, pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        return getStore().getVertexIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                      IStatePushDown pushdown) {
        return getStore().getVertexIterator(version, keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        return getStore().getEdgeIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                  IStatePushDown pushdown) {
        return getStore().getEdgeIterator(version, keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                         IStatePushDown pushdown) {
        return getStore().getOneDegreeGraphIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                         IStatePushDown pushdown) {
        return getStore().getOneDegreeGraphIterator(version, keys, pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        return getStore().getAllVersions(id, dataType);
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        return getStore().getLatestVersion(id, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        return getStore().getAllVersionData(id, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                                    IStatePushDown pushdown, DataType dataType) {
        return getStore().getVersionData(id, versions, pushdown, dataType);
    }
}
