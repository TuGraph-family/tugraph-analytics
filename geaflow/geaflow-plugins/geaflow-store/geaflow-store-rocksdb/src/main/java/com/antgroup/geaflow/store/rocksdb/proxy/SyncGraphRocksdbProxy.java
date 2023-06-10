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

package com.antgroup.geaflow.store.rocksdb.proxy;

import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.EDGE_CF;
import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_CF;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.IEdgeKVEncoder;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.graph.encoder.IVertexKVEncoder;
import com.antgroup.geaflow.state.iterator.IteratorWithFlatFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFnThenFilter;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.iterator.KeysIterator;
import com.antgroup.geaflow.store.rocksdb.RocksdbClient;
import com.antgroup.geaflow.store.rocksdb.iterator.EdgeListScanIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.EdgeScanIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.OneDegreeGraphScanIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.RocksdbIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.VertexScanIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class SyncGraphRocksdbProxy<K, VV, EV> implements IGraphRocksdbProxy<K, VV, EV> {

    protected final Configuration config;
    protected final IVertexKVEncoder<K,VV> vertexEncoder;
    protected final IEdgeKVEncoder<K,EV> edgeEncoder;
    protected IGraphKVEncoder<K, VV, EV> encoder;
    protected final RocksdbClient rocksdbClient;

    public SyncGraphRocksdbProxy(RocksdbClient rocksdbClient, IGraphKVEncoder<K, VV, EV> encoder,
                                 Configuration config) {
        this.encoder = encoder;
        this.vertexEncoder = this.encoder.getVertexEncoder();
        this.edgeEncoder = this.encoder.getEdgeEncoder();
        this.rocksdbClient = rocksdbClient;
        this.config = config;
    }

    @Override
    public RocksdbClient getClient() {
        return rocksdbClient;
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        this.rocksdbClient.write(VERTEX_CF, tuple.f0, tuple.f1);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        this.rocksdbClient.write(EDGE_CF, tuple.f0, tuple.f1);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        byte[] value = this.rocksdbClient.get(VERTEX_CF, key);
        if (value != null) {
            IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
            if (pushdown == null || ((IGraphFilter)pushdown.getFilter()).filterVertex(vertex)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        return getEdges(sid, filter);
    }

    protected List<IEdge<K, EV>> getEdges(K sid, IGraphFilter filter) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        byte[] prefix = edgeEncoder.getScanBytes(sid);
        try (RocksdbIterator it =
            new RocksdbIterator(this.rocksdbClient.getIterator(EDGE_CF), prefix)) {
            while (it.hasNext()) {
                Tuple<byte[], byte[]> pair = it.next();
                IEdge<K, EV> edge = edgeEncoder.getEdge(pair.f0, pair.f1);
                if (filter.filterEdge(edge)) {
                    list.add(edge);
                }
                if (filter.dropAllRemaining()) {
                    break;
                }
            }
        }
        return list;
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid, pushdown);
        List<IEdge<K, EV>> edgeList = getEdges(sid, pushdown);
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex, edgeList.iterator());
        if (filter.filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        } else {
            return null;
        }
    }

    @Override
    public Iterator<K> vertexIDIterator() {
        flush();
        RocksdbIterator it = new RocksdbIterator(this.rocksdbClient.getIterator(VERTEX_CF));
        return new IteratorWithFnThenFilter<>(it, tuple2 -> vertexEncoder.getVertexID(tuple2.f0),
            new Predicate<K>() {
                K last = null;
                @Override
                public boolean test(K k) {
                    boolean res = k.equals(last);
                    last = k;
                    return !res;
                }
            });
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        flush();
        RocksdbIterator it = new RocksdbIterator(rocksdbClient.getIterator(VERTEX_CF));
        return new VertexScanIterator<>(it, pushdown, vertexEncoder::getVertex);
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getVertex, pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        flush();
        RocksdbIterator it = new RocksdbIterator(rocksdbClient.getIterator(EDGE_CF));
        return new EdgeScanIterator<>(it, pushdown, edgeEncoder::getEdge);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(new KeysIterator<>(keys, this::getEdges, pushdown), List::iterator);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(encoder.getKeyType(),
            getVertexIterator(pushdown), getEdgeIterator(pushdown), pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getOneDegreeGraph, pushdown);
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        flush();
        return new IteratorWithFn<>(getEdgeIterator(pushdown), e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                            IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(keys, pushdown), e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Iterator<List<IEdge<K, EV>>> it =
            new EdgeListScanIterator<>(getEdgeIterator(pushdown));
        while (it.hasNext()) {
            List<IEdge<K, EV>> edges = it.next();
            K key = edges.get(0).getSrcId();
            res.put(key, (long) edges.size());
        }
        return res;
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>(keys.size());

        Function<K, IStatePushDown> pushdownFun;
        if (pushdown.getFilters() == null) {
            pushdownFun = key -> pushdown;
        } else {
            pushdownFun =
                key -> StatePushDown.of().withFilter((IFilter) pushdown.getFilters().get(key));
        }

        for (K key: keys) {
            List<IEdge<K, EV>> list = getEdges(key, pushdownFun.apply(key));
            res.put(key, (long) list.size());
        }
        return res;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

}
