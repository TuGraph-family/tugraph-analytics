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
import com.antgroup.geaflow.state.iterator.IteratorWithFnThenFilter;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class GraphMemoryStore<K, VV, EV> extends BaseGraphMemoryStore<K, VV, EV> {

    protected Map<K, Tuple<IVertex<K, VV>, List<IEdge<K, EV>>>> map;

    public GraphMemoryStore() {
    }

    @Override
    public void init(StoreContext context) {
        super.init(context);
        map = new ConcurrentHashMap<>();
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        K srcId = edge.getSrcId();
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.computeIfAbsent(srcId,
            k -> Tuple.of(null, new ArrayList<>()));
        v.f1.add(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        K srcId = vertex.getId();
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.computeIfAbsent(srcId,
            k -> Tuple.of(null, new ArrayList<>()));
        v.f0 = vertex;
    }

    @Override
    protected IVertex<K, VV> getVertex(K sid) {
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.get(sid);
        return v == null ? null : v.f0;
    }

    @Override
    protected List<IEdge<K, EV>> getEdges(K sid) {
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.get(sid);
        return v == null ? Collections.EMPTY_LIST : v.f1;
    }

    @Override
    protected Iterator<List<IEdge<K, EV>>> getEdgesIterator() {
        return map.values().stream().map(c -> c.f1).iterator();
    }

    @Override
    protected Iterator<IVertex<K, VV>> getVertexIterator() {
        return new IteratorWithFnThenFilter<>(map.values().iterator(), c -> c.f0, Objects::nonNull);
    }

    @Override
    protected Iterator<K> getKeyIterator() {
        return map.keySet().iterator();
    }

    @Override
    public void close() {

    }

    @Override
    public void drop() {

    }

}
