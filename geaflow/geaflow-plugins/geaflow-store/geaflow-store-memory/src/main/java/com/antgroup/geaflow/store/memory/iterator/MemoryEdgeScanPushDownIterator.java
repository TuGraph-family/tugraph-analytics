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

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MemoryEdgeScanPushDownIterator<K, VV, EV> implements Iterator<List<IEdge<K, EV>>> {

    private final Iterator<List<IEdge<K, EV>>> iterator;
    private final Comparator<IEdge> edgeComparator;
    private final IEdgeLimit edgeLimit;
    private final IGraphFilter filter;
    private List<IEdge<K, EV>> nextValue;

    public MemoryEdgeScanPushDownIterator(
        Iterator<List<IEdge<K, EV>>> iterator,
        IStatePushDown pushdown) {
        this.filter = (IGraphFilter) pushdown.getFilter();
        this.edgeLimit = pushdown.getEdgeLimit();
        this.edgeComparator = pushdown.getOrderField() == null ? null : pushdown.getOrderField().getComparator();
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            List<IEdge<K, EV>> list = Lists.newArrayList(iterator.next());
            if (this.edgeComparator != null) {
                list.sort(this.edgeComparator);
            }
            List<IEdge<K, EV>> res = new ArrayList<>(list.size());
            Iterator<IEdge<K, EV>> it = list.iterator();
            IGraphFilter filter = GraphFilter.of(this.filter, this.edgeLimit);
            while (it.hasNext() && !filter.dropAllRemaining()) {
                IEdge<K, EV> edge = it.next();
                if (filter.filterEdge(edge)) {
                    res.add(edge);
                }
            }

            nextValue = res;
            return true;
        }
        return false;
    }

    @Override
    public List<IEdge<K, EV>> next() {
        return nextValue;
    }
}
