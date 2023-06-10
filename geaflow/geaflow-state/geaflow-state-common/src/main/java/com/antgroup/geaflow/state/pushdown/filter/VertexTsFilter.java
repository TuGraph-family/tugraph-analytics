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

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.TimeRange;

public class VertexTsFilter<K, VV> implements IVertexFilter<K, VV> {

    private final TimeRange timeRange;

    public VertexTsFilter(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public static <K, VV> VertexTsFilter<K, VV> instance(long start, long end) {
        return new VertexTsFilter<>(TimeRange.of(start, end));
    }

    @Override
    public boolean filter(IVertex<K, VV> value) {
        return timeRange.contain(((IGraphElementWithTimeField)value).getTime());
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.VERTEX_TS;
    }

    @Override
    public String toString() {
        return String.format("\"%s[%d,%d)\"", getFilterType().toString(),
            timeRange.getStart(), timeRange.getEnd());
    }
}
