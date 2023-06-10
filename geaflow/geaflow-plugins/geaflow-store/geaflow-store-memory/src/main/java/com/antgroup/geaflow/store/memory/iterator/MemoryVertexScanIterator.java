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

package com.antgroup.geaflow.store.memory.iterator;

import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.iterator.IVertexIterator;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import java.util.Iterator;

public class MemoryVertexScanIterator<K, VV, EV> implements IVertexIterator<K, VV> {

    private final Iterator<IVertex<K,VV>> iterator;
    private final IGraphFilter filter;
    private IVertex<K, VV> nextValue;

    public MemoryVertexScanIterator(Iterator<IVertex<K,VV>> iterator, IGraphFilter filter) {
        this.iterator = iterator;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            nextValue = iterator.next();
            if (!filter.filterVertex(nextValue)) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public IVertex<K, VV> next() {
        return nextValue;
    }
}
