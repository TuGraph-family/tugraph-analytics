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
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.LimitFilterBuilder;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class EdgeScanIterator<K, VV, EV> implements Iterator<IEdge<K, EV>> {

    private Supplier<IGraphFilter> filterFun;
    private final Iterator<Tuple<byte[], byte[]>> iterator;
    private final BiFunction<byte[], byte[], IEdge<K,EV>> edgeDecoder;
    private IEdge<K, EV> nextValue;
    private K lastKey = null;
    private IGraphFilter filter = null;

    public EdgeScanIterator(
        Iterator<Tuple<byte[], byte[]>> iterator,
        IStatePushDown pushdown,
        BiFunction<byte[], byte[], IEdge<K, EV>> decoderFun) {

        IGraphFilter filter = (IGraphFilter) pushdown.getFilter();
        IEdgeLimit limit = pushdown.getEdgeLimit();
        filterFun = limit == null ? () -> filter : () -> LimitFilterBuilder.build(filter, limit);
        this.iterator = iterator;
        this.edgeDecoder = decoderFun;
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            Tuple<byte[], byte[]> pair = iterator.next();

            nextValue = edgeDecoder.apply(pair.f0, pair.f1);
            if (!nextValue.getSrcId().equals(lastKey)) {
                filter = filterFun.get();
                lastKey = nextValue.getSrcId();
            }
            if (!filter.filterEdge(nextValue)) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public IEdge<K, EV> next() {
        return nextValue;
    }
}
