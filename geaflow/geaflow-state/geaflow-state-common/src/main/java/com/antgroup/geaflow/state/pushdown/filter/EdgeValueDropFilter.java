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

package com.antgroup.geaflow.state.pushdown.filter;

import com.antgroup.geaflow.model.graph.edge.IEdge;

public class EdgeValueDropFilter<K, VV> implements IEdgeFilter<K, VV> {

    private static final EdgeValueDropFilter filter = new EdgeValueDropFilter();

    public static <K, EV> EdgeValueDropFilter<K, EV> instance() {
        return filter;
    }

    @Override
    public boolean filter(IEdge<K, VV> value) {
        value.withValue(null);
        return true;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.EDGE_VALUE_DROP;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }
}
