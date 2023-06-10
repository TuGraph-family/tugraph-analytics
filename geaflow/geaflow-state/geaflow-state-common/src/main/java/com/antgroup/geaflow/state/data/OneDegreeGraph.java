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

package com.antgroup.geaflow.state.data;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.io.Serializable;
import java.util.Iterator;

public class OneDegreeGraph<K, VV, EV> implements Serializable {

    private IVertex<K, VV> vertex;
    protected Iterator<IEdge<K, EV>> edgeIterator;
    protected K key;

    public OneDegreeGraph(K key, IVertex<K, VV> vertex, Iterator<IEdge<K, EV>> edgeIterator) {
        this.key = key;
        this.vertex = vertex;
        this.edgeIterator = edgeIterator;
    }

    public K getKey() {
        return key;
    }

    public IVertex<K, VV> getVertex() {
        return vertex;
    }

    public Iterator<IEdge<K, EV>> getEdgeIterator() {
        return edgeIterator;
    }
}

