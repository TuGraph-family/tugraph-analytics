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

package com.antgroup.geaflow.state.pushdown.inner;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.pushdown.filter.AndFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.OrFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexTsFilter;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.FilterNode;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.IntList;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.LongList;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.StringList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FilterGenerator {

    public static FilterPlanWithData getFilterPlanWithData(IFilter filter) {
        FilterNode plan = getFilterPlan(filter);
        FilterNode data = getFilterData(filter);
        return new FilterPlanWithData(plan, data);
    }

    private static FilterNode getFilterPlan(IFilter filter) {
        FilterType filterType = filter.getFilterType();
        switch (filterType) {
            case AND:
                return getLogicalPlan(filterType, ((AndFilter)filter).getFilters());
            case OR:
                return getLogicalPlan(filterType, ((OrFilter)filter).getFilters());
            case VERTEX_LABEL:
            case EDGE_LABEL:
            case VERTEX_TS:
            case EDGE_TS:
            case IN_EDGE:
            case OUT_EDGE:
            case VERTEX_VALUE_DROP:
            case EDGE_VALUE_DROP:
            case VERTEX_MUST_CONTAIN:
                return getNormalPlan(filterType);
            default:
                throw new GeaflowRuntimeException("not support user defined filter " + filter.getFilterType());
        }
    }

    private static FilterNode getLogicalPlan(FilterType filterType, List<IFilter> filters) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .addAllFilters(filters.stream()
                .sorted(Comparator.comparingInt(o -> o.getFilterType().ordinal()))
                .map(FilterGenerator::getFilterPlan).collect(Collectors.toList()))
            .build();
    }

    private static FilterNode getNormalPlan(FilterType filterType) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .build();
    }

    private static FilterNode getFilterData(IFilter filter) {
        FilterType filterType = filter.getFilterType();
        switch (filterType) {
            case AND:
                return getLogicalFilterData(filterType, ((AndFilter)filter).getFilters());
            case OR:
                return getLogicalFilterData(filterType, ((OrFilter)filter).getFilters());
            case VERTEX_LABEL:
                return getStringFilterData(filterType, ((VertexLabelFilter)filter).getLabels());
            case EDGE_LABEL:
                return getStringFilterData(filterType, ((EdgeLabelFilter)filter).getLabels());
            case VERTEX_TS:
                TimeRange range = ((VertexTsFilter)filter).getTimeRange();
                Collection<Long> longs = Arrays.asList(range.getStart(), range.getEnd());
                return getLongFilterData(filterType, longs);
            case EDGE_TS:
                range = ((EdgeTsFilter)filter).getTimeRange();
                longs = Arrays.asList(range.getStart(), range.getEnd());
                return getLongFilterData(filterType, longs);
            case IN_EDGE:
            case OUT_EDGE:
            case VERTEX_VALUE_DROP:
            case EDGE_VALUE_DROP:
            case VERTEX_MUST_CONTAIN:
                return FilterNode.newBuilder()
                    .setFilterOrdinal(filterType.ordinal())
                    .build();
            default:
                throw new GeaflowRuntimeException("not support user defined filter " + filter.getFilterType());
        }
    }

    private static FilterNode getLogicalFilterData(FilterType filterType, List<IFilter> filters) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .addAllFilters(filters.stream()
                .sorted(Comparator.comparingInt(o -> o.getFilterType().ordinal()))
                .map(FilterGenerator::getFilterData).collect(Collectors.toList()))
            .build();
    }

    private static FilterNode getStringFilterData(FilterType filterType, Collection<String> strs) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .setStrContent(StringList.newBuilder().addAllStr(strs).build())
            .build();
    }

    private static FilterNode getLongFilterData(FilterType filterType, Collection<Long> longs) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .setLongContent(LongList.newBuilder().addAllLong(longs).build())
            .build();
    }

    private static FilterNode getIntFilterData(FilterType filterType, Collection<Integer> ints) {
        return FilterNode.newBuilder()
            .setFilterOrdinal(filterType.ordinal())
            .setIntContent(IntList.newBuilder().addAllInt(ints).build())
            .build();
    }
}
