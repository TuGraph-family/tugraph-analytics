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

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.iterator.IVertexIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import java.util.Iterator;
import java.util.function.BiFunction;

public class VertexScanIterator<K, VV, EV> implements IVertexIterator<K, VV> {

    private final Iterator<Tuple<byte[], byte[]>> iterator;
    private final IGraphFilter filter;
    private final BiFunction<byte[], byte[], IVertex<K,VV>> vertexDecoder;
    private boolean isClosed = false;
    private IVertex<K, VV> nextValue;

    public VertexScanIterator(Iterator<Tuple<byte[], byte[]>> iterator,
                              IStatePushDown pushdown,
                              BiFunction<byte[], byte[], IVertex<K, VV>> decoderFun) {
        this.vertexDecoder = decoderFun;
        this.iterator = iterator;
        this.filter = (IGraphFilter) pushdown.getFilter();
    }

    @Override
    public boolean hasNext() {
        if (isClosed) {
            return false;
        }
        while (iterator.hasNext()) {
            Tuple<byte[], byte[]> pair = iterator.next();
            nextValue = vertexDecoder.apply(pair.f0, pair.f1);

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
