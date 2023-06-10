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
import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_INDEX_CF;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.GraphKVEncoderFactory;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.store.api.graph.IGraphMultiVersionedStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.rocksdb.proxy.IGraphMultiVersionedRocksdbProxy;
import com.antgroup.geaflow.store.rocksdb.proxy.ProxyBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphMultiVersionedRocksdbStore<K, VV, EV> extends BaseRocksdbGraphStore
    implements IGraphMultiVersionedStore<K, VV, EV> {

    private IGraphMultiVersionedRocksdbProxy<K, VV, EV> proxy;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(config,
            storeContext.getGraphSchema());
        this.proxy = ProxyBuilder.buildMultiVersioned(config, rocksdbClient, encoder);
    }

    @Override
    protected List<String> getCfList() {
        return Arrays.asList(VERTEX_CF, EDGE_CF, VERTEX_INDEX_CF);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        this.proxy.addEdge(version, edge);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        this.proxy.addVertex(version, vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(long sliceId, K sid, IStatePushDown pushdown) {
        return this.proxy.getVertex(sliceId, sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long sliceId, K sid, IStatePushDown pushdown) {
        return this.proxy.getEdges(sliceId, sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long sliceId, K sid,
                                                       IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraph(sliceId, sid, pushdown);
    }

    @Override
    public Iterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        return proxy.getVertexIterator(version, pushdown);
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                      IStatePushDown pushdown) {
        return proxy.getVertexIterator(version, keys, pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        return proxy.getEdgeIterator(version, pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                  IStatePushDown pushdown) {
        return proxy.getEdgeIterator(version, keys, pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                         IStatePushDown pushdown) {
        return proxy.getOneDegreeGraphIterator(version, pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                         IStatePushDown pushdown) {
        return proxy.getOneDegreeGraphIterator(version, keys, pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        return this.proxy.getAllVersions(id, dataType);
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        return this.proxy.getLatestVersion(id, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        return this.proxy.getAllVersionData(id, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> slices,
                                                    IStatePushDown pushdown, DataType dataType) {
        return this.proxy.getVersionData(id, slices, pushdown, dataType);
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
