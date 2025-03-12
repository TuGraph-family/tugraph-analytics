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

package com.antgroup.geaflow.store.paimon.proxy;

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.iterator.IteratorWithClose;
import com.antgroup.geaflow.state.iterator.IteratorWithFlatFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFnThenFilter;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.iterator.EdgeListScanIterator;
import com.antgroup.geaflow.store.iterator.EdgeScanIterator;
import com.antgroup.geaflow.store.iterator.KeysIterator;
import com.antgroup.geaflow.store.iterator.OneDegreeGraphScanIterator;
import com.antgroup.geaflow.store.iterator.VertexScanIterator;
import com.antgroup.geaflow.store.paimon.PaimonTableRWHandle;
import com.antgroup.geaflow.store.paimon.iterator.PaimonIterator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;

public abstract class PaimonBaseGraphProxy<K, VV, EV> implements IGraphPaimonProxy<K, VV, EV> {

    protected static final int KEY_COLUMN_INDEX = 0;

    protected static final int VALUE_COLUMN_INDEX = 1;

    protected IGraphKVEncoder<K, VV, EV> encoder;

    protected PaimonTableRWHandle vertexHandle;

    protected PaimonTableRWHandle edgeHandle;

    protected int[] projection;

    protected long lastCheckpointId;

    public PaimonBaseGraphProxy(PaimonTableRWHandle vertexHandle, PaimonTableRWHandle edgeHandle,
                                int[] projection, IGraphKVEncoder<K, VV, EV> encoder) {
        this.vertexHandle = vertexHandle;
        this.edgeHandle = edgeHandle;
        this.projection = projection;
        this.encoder = encoder;
        this.lastCheckpointId = 0;
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> tuple = this.encoder.getEdgeEncoder().format(edge);
        GenericRow record = GenericRow.of(tuple.f0, tuple.f1);
        this.edgeHandle.write(record, 0);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = this.encoder.getVertexEncoder().format(vertex);
        GenericRow record = GenericRow.of(tuple.f0, tuple.f1);
        this.vertexHandle.write(record, 0);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid, pushdown);
        List<IEdge<K, EV>> edgeList = getEdges(sid, pushdown);
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex,
            IteratorWithClose.wrap(edgeList.iterator()));
        if (filter.filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        } else {
            return null;
        }
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        flush();
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(null,
            projection);
        PaimonIterator it = new PaimonIterator(iterator);
        return new IteratorWithFnThenFilter<>(it,
            tuple2 -> encoder.getVertexEncoder().getVertexID(tuple2.f0), new Predicate<K>() {
                K last = null;

                @Override
                public boolean test(K k) {
                    boolean res = k.equals(last);
                    last = k;
                    return !res;
                }
            }
        );
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        if (pushDown.getFilter() == null) {
            return vertexIDIterator();
        } else {
            return new IteratorWithFn<>(getVertexIterator(pushDown), IVertex::getId);
        }
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        flush();
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(null,
            projection);
        PaimonIterator it = new PaimonIterator(iterator);
        return new VertexScanIterator<>(it, pushdown, encoder.getVertexEncoder()::getVertex);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys,
                                                               IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getVertex, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        flush();
        RecordReaderIterator<InternalRow> iterator = this.edgeHandle.getIterator(null, projection);
        PaimonIterator it = new PaimonIterator(iterator);
        return new EdgeScanIterator<>(it, pushdown, encoder.getEdgeEncoder()::getEdge);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(new KeysIterator<>(keys, this::getEdges, pushdown),
            List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(encoder.getKeyType(), getVertexIterator(pushdown),
            getEdgeIterator(pushdown), pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getOneDegreeGraph, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        flush();
        return new IteratorWithFn<>(getEdgeIterator(pushdown),
            e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K,
                                                                         EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(keys, pushdown),
            e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Iterator<List<IEdge<K, EV>>> it = new EdgeListScanIterator<>(getEdgeIterator(pushdown));
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
            pushdownFun = key -> StatePushDown.of()
                .withFilter((IFilter) pushdown.getFilters().get(key));
        }

        for (K key : keys) {
            List<IEdge<K, EV>> list = getEdges(key, pushdownFun.apply(key));
            res.put(key, (long) list.size());
        }
        return res;
    }

    @Override
    public void flush() {
        this.vertexHandle.flush(lastCheckpointId);
        this.edgeHandle.flush(lastCheckpointId);
    }

    @Override
    public void close() {
        this.vertexHandle.close();
        this.edgeHandle.close();
    }
}
