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

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.iterator.IteratorWithFlatFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFn;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.BaseGraphStore;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.iterator.KeysIterator;
import com.antgroup.geaflow.store.memory.iterator.MemoryEdgeScanPushDownIterator;
import com.antgroup.geaflow.store.memory.iterator.MemoryVertexScanIterator;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class BaseGraphMemoryStore<K, VV, EV> extends BaseGraphStore implements IGraphStore<K, VV, EV> {

    @Override
    public void init(StoreContext context) {
        super.init(context);
    }

    protected abstract IVertex<K, VV> getVertex(K sid);

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid);
        return vertex != null && ((IGraphFilter)pushdown.getFilter()).filterVertex(vertex) ? vertex : null;
    }

    protected abstract List<IEdge<K, EV>> getEdges(K sid);

    protected List<IEdge<K, EV>> pushdownEdges(List<IEdge<K, EV>> list, IStatePushDown pushdown) {
        if (pushdown.getOrderField() != null) {
            list.sort(pushdown.getOrderField().getComparator());
        }
        List<IEdge<K, EV>> res = new ArrayList<>(list.size());
        Iterator<IEdge<K, EV>> it = list.iterator();
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        while (it.hasNext() && !filter.dropAllRemaining()) {
            IEdge<K, EV> edge = it.next();
            if (filter.filterEdge(edge)) {
                res.add(edge);
            }
        }

        return res;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        return pushdownEdges(getEdges(sid), pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid, pushdown);
        List<IEdge<K, EV>> edges = getEdges(sid, pushdown);
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex, edges.iterator());
        if (((IGraphFilter)pushdown.getFilter()).filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        }
        return null;
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        boolean emptyFilter = pushdown.getFilter().getFilterType() == FilterType.EMPTY;
        return emptyFilter ? getVertexIterator() :
               new MemoryVertexScanIterator<>(getVertexIterator(), (IGraphFilter)pushdown.getFilter());
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(List<K> list, IStatePushDown pushdown) {
        return new KeysIterator<>(list, this::getVertex, pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        Iterator<List<IEdge<K, EV>>> it = new MemoryEdgeScanPushDownIterator<>(getEdgesIterator(), pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(List<K> list, IStatePushDown pushdown) {
        Iterator<List<IEdge<K, EV>>> it = new KeysIterator<>(list, this::getEdges, pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return new KeysIterator<>(Lists.newArrayList(getKeyIterator()), this::getOneDegreeGraph, pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getOneDegreeGraph, pushdown);
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(pushdown),
            edge -> Tuple.of(edge.getSrcId(), pushdown.getProjector().project(edge)));
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                            IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(keys, pushdown),
            edge -> Tuple.of(edge.getSrcId(), pushdown.getProjector().project(edge)));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Iterator<K> keyIt = getKeyIterator();
        while (keyIt.hasNext()) {
            K key = keyIt.next();

            List<IEdge<K, EV>> list = getEdges(key, pushdown);
            res.put(key, (long)list.size());
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
                key -> StatePushDown.of().withFilter((IGraphFilter) pushdown.getFilters().get(key));
        }

        for (K key: keys) {
            List<IEdge<K, EV>> list = getEdges(key, pushdownFun.apply(key));
            res.put(key, (long) list.size());
        }
        return res;
    }

    protected abstract Iterator<List<IEdge<K, EV>>> getEdgesIterator();

    protected abstract Iterator<IVertex<K, VV>> getVertexIterator();

    @Override
    public Iterator<K> vertexIDIterator() {
        return new IteratorWithFn<>(getVertexIterator(), IVertex::getId);
    }

    protected abstract Iterator<K> getKeyIterator();
}
