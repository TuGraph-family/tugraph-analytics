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

package com.antgroup.geaflow.store.rocksdb.iterator;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.iterator.IOneDegreeGraphIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OneDegreeGraphScanIterator<K, VV, EV> implements
    IOneDegreeGraphIterator<K, VV, EV> {

    private final Iterator<IVertex<K, VV>> vertexIterator;
    private final Iterator<List<IEdge<K, EV>>> edgeListIterator;
    private final IType<K> keyType;
    private IGraphFilter filter;
    private OneDegreeGraph<K, VV, EV> nextValue;

    private List<IEdge<K, EV>> candidateEdges;
    private IVertex<K, VV> candidateVertex;

    public OneDegreeGraphScanIterator(
        IType<K> keyType,
        Iterator<IVertex<K, VV>> vertexIterator,
        Iterator<IEdge<K, EV>> edgeIterator,
        IStatePushDown pushdown) {
        this.keyType = keyType;
        this.vertexIterator = vertexIterator;
        this.edgeListIterator = new EdgeListScanIterator<>(edgeIterator);
        this.filter = (IGraphFilter) pushdown.getFilter();
    }


    private IVertex<K, VV> getVertexFromIterator() {
        if (vertexIterator.hasNext()) {
            return vertexIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        do {
            candidateVertex = candidateVertex == null ? getVertexFromIterator() : candidateVertex;
            if (candidateEdges == null && edgeListIterator.hasNext()) {
                candidateEdges = edgeListIterator.next();
            } else if (candidateEdges == null) {
                candidateEdges = new ArrayList<>();
            }

            if (candidateEdges.size() > 0 && candidateVertex != null) {
                K edgeKey = candidateEdges.get(0).getSrcId();
                K vertexKey = candidateVertex.getId();
                int res = keyType.compare(edgeKey, vertexKey);
                if (res < 0) {
                    nextValue = new OneDegreeGraph<>(edgeKey, null, candidateEdges.iterator());
                    candidateEdges = null;
                } else if (res == 0) {
                    nextValue = new OneDegreeGraph<>(vertexKey, candidateVertex, candidateEdges.iterator());
                    candidateVertex = null;
                    candidateEdges = null;
                } else {
                    nextValue = new OneDegreeGraph<>(vertexKey, candidateVertex, Collections.emptyIterator());
                    candidateVertex = null;
                }
            } else if (candidateEdges.size() > 0) {
                nextValue = new OneDegreeGraph<>(candidateEdges.get(0).getSrcId(), null,
                    candidateEdges.iterator());
                candidateEdges = null;
            } else if (candidateVertex != null) {
                nextValue = new OneDegreeGraph<>(candidateVertex.getId(), candidateVertex, Collections.emptyIterator());
                candidateVertex = null;
            } else {
                return false;
            }

            if (!filter.filterOneDegreeGraph(nextValue)) {
                continue;
            }
            return true;
        } while (true);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> next() {
        return nextValue;
    }
}
