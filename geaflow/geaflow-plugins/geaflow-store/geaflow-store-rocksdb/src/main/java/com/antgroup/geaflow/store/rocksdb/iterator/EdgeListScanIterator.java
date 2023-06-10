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

import com.antgroup.geaflow.model.graph.edge.IEdge;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EdgeListScanIterator<K, EV> implements Iterator<List<IEdge<K, EV>>> {

    private final Iterator<IEdge<K, EV>> edgeIterator;
    private IEdge<K, EV> residualEdge;
    private List<IEdge<K, EV>> nextValue;

    public EdgeListScanIterator(Iterator<IEdge<K, EV>> edgeIterator) {
        this.edgeIterator = edgeIterator;
    }

    @Override
    public boolean hasNext() {
        nextValue = getEdgesFromIterator();
        if (nextValue.size() == 0) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public List<IEdge<K, EV>> next() {
        return nextValue;
    }

    private List<IEdge<K, EV>> getEdgesFromIterator() {
        List<IEdge<K, EV>> list = new ArrayList<>();
        final IEdge<K, EV> lastResidualEdge = residualEdge;
        K key = null;
        if (residualEdge != null) {
            list.add(residualEdge);
            key = residualEdge.getSrcId();
        }
        while (edgeIterator.hasNext()) {
            IEdge<K, EV> edge = edgeIterator.next();
            if (key == null) {
                key = edge.getSrcId();
            }
            if (edge.getSrcId().equals(key)) {
                list.add(edge);
            } else {
                residualEdge = edge;
                break;
            }
        }
        if (lastResidualEdge == residualEdge) {
            residualEdge = null;
        }
        return list;
    }
}
