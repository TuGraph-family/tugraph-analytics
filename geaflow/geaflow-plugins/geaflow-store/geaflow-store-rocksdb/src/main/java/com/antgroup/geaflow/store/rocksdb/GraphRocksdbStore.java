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

package com.antgroup.geaflow.store.rocksdb;

import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.EDGE_CF;
import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_CF;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.graph.encoder.GraphKVEncoderFactory;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.rocksdb.proxy.IGraphRocksdbProxy;
import com.antgroup.geaflow.store.rocksdb.proxy.ProxyBuilder;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphRocksdbStore<K, VV, EV> extends BaseRocksdbGraphStore
    implements IGraphStore<K, VV, EV> {

    private IGraphRocksdbProxy<K, VV, EV> proxy;
    private EdgeAtom sortAtom;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(config,
            storeContext.getGraphSchema());
        sortAtom = storeContext.getGraphSchema().getEdgeAtoms().get(1);
        this.proxy = ProxyBuilder.build(config, rocksdbClient, encoder);
    }

    @Override
    protected List<String> getCfList() {
        return Arrays.asList(VERTEX_CF, EDGE_CF);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        this.proxy.addEdge(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        this.proxy.addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return this.proxy.getVertex(sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public Iterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return proxy.getVertexIterator(pushdown);
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return proxy.getVertexIterator(keys, pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getEdgeIterator(pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getEdgeIterator(keys, pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getOneDegreeGraphIterator(pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderField());
        return proxy.getOneDegreeGraphIterator(keys, pushdown);
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return proxy.getEdgeProjectIterator(pushdown);
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys, IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return proxy.getEdgeProjectIterator(keys, pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        return proxy.getAggResult(pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        return proxy.getAggResult(keys, pushdown);
    }

    private void checkOrderField(EdgeAtom orderField) {
        Preconditions.checkArgument(orderField == null || sortAtom == orderField,
            "store is sort by %s but need %s", sortAtom, orderField);
    }

    @Override
    public void flush() {
        proxy.flush();
        super.flush();
    }

    @Override
    public void close() {
        proxy.close();
        super.close();
    }
}
