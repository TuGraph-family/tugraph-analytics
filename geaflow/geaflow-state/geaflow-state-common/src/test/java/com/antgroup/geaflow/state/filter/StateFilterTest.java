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

package com.antgroup.geaflow.state.filter;

import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDTimeEdge;
import com.antgroup.geaflow.model.graph.vertex.impl.IDVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.pushdown.filter.AndFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeValueDropFilter;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexValueDropFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.EmptyGraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StateFilterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StateFilterTest.class);
    
    @Test
    public void testValidate() {
        EdgeTsFilter tsFilter = new EdgeTsFilter(TimeRange.of(100, 200));
        Exception e = null;
        try {
            IFilter filter = tsFilter.or(new EdgeValueDropFilter());
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new VertexTsFilter(TimeRange.of(100, 200)));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new VertexTsFilter(TimeRange.of(100, 200)).and(tsFilter));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter()));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new EdgeValueDropFilter());
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.and(new EdgeValueDropFilter()).or(new EdgeTsFilter(TimeRange.of(100, 200)));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);


        IFilter filter =
            tsFilter.and(new EdgeValueDropFilter()).or(new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter()));
        filter =
            new EdgeValueDropFilter().and(new VertexValueDropFilter())
                .or(new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter()).and(new VertexValueDropFilter()));
    }

    @Test
    public void testEdgeTsFilter() {
        EdgeTsFilter filter = new EdgeTsFilter(TimeRange.of(100, 200));
        Assert.assertEquals(filter.getFilterType(), FilterType.EDGE_TS);
        IEdge<String, Object> edge = new IDTimeEdge<>("hello", "world", 99);
        Assert.assertFalse(filter.filter(edge));

        IGraphFilter complexFilter = GraphFilter.of(filter);
        Assert.assertFalse(complexFilter.filterEdge(edge));
    }

    @Test
    public void testEdgeDirection() {
        IEdge<String, Object> edge1 = new IDEdge<>("hello", "world");
        edge1.setDirect(EdgeDirection.OUT);
        IEdge<String, Object> edge2 = new IDEdge<>("hello", "world");
        edge2.setDirect(EdgeDirection.IN);

        IGraphFilter filter = GraphFilter.of(new InEdgeFilter());
        Assert.assertFalse(filter.filterEdge(edge1));
        Assert.assertTrue(filter.filterEdge(edge2));

        filter = GraphFilter.of(new OutEdgeFilter());
        Assert.assertTrue(filter.filterEdge(edge1));
        Assert.assertFalse(filter.filterEdge(edge2));
    }

    @Test
    public void testAndFilter() {
        IFilter filter = new EdgeTsFilter(TimeRange.of(1L, 2L));
        IFilter appendFilter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(1L, 2L)));
        filter = filter.and(appendFilter);
        Assert.assertEquals(appendFilter.getFilterType(), FilterType.AND);
        Assert.assertEquals(filter.toString(), filter.and(EmptyGraphFilter.of()).toString());

        Assert.assertEquals(((AndFilter)appendFilter).getFilters().size(), 2);
        Assert.assertEquals(filter.getFilterType(), FilterType.AND);
        Assert.assertEquals(((AndFilter)filter).getFilters().size(), 3);

        filter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(100, 200)));
        IEdge<String, Object> edge = new IDTimeEdge<>("hello", "world", 100);
        IGraphFilter stateFilter = GraphFilter.of(filter);
        Assert.assertFalse(stateFilter.filterEdge(edge));

        edge.setDirect(EdgeDirection.IN);
        Assert.assertTrue(stateFilter.filterEdge(edge));
        Assert.assertFalse(stateFilter.dropAllRemaining());

        List<IEdge<String, Object>> list = Arrays.asList(
            new IDTimeEdge<>("hello", "world", 100),
            new IDTimeEdge<>("hello", "world", 120),
            new IDTimeEdge<>("hello", "world", 140),
            new IDTimeEdge<>("hello", "world", 160),
            new IDTimeEdge<>("hello", "world", 180));

        list.get(0).setDirect(EdgeDirection.IN);
        list.get(2).setDirect(EdgeDirection.IN);
        list.get(4).setDirect(EdgeDirection.IN);

        filter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(80, 141)));
        stateFilter = GraphFilter.of(filter);
        LOGGER.info("filter {}", stateFilter.toString());
        IGraphFilter graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.contains(FilterType.EDGE_TS));
        Assert.assertEquals(graphFilter.retrieve(FilterType.EDGE_TS).getFilterType(), FilterType.EDGE_TS);
        List<IEdge<String, Object>> res = list.stream().filter(stateFilter::filterEdge)
            .collect(Collectors.toList());
        Assert.assertEquals(res.size(), 2);
        Assert.assertTrue(res.contains(list.get(0)));
        Assert.assertTrue(res.contains(list.get(2)));

        graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.filterVertex(new IDVertex("1")));
        Assert.assertTrue(graphFilter.filterOneDegreeGraph(new OneDegreeGraph<>("1", null, null)));
    }

    @Test
    public void testOrFilter() {
        IFilter filter = new EdgeTsFilter(TimeRange.of(1L, 2L));
        filter = filter.or(new EdgeTsFilter(TimeRange.of(3L, 4L)));

        Assert.assertEquals(filter.getFilterType(), FilterType.OR);
        Assert.assertEquals(filter.or(EmptyGraphFilter.of()).toString(), filter.toString());

        List<IEdge<String, Object>> list = Arrays.asList(
            new IDTimeEdge<>("hello", "world", 1),
            new IDTimeEdge<>("hello", "world", 3),
            new IDTimeEdge<>("hello", "world", 5)
        );

        IGraphFilter graphFilter = GraphFilter.of(filter);
        List<IEdge<String, Object>> res = new ArrayList<>();
        for (IEdge<String, Object> stringObjectIEdge : list) {
            if (graphFilter.filterEdge(stringObjectIEdge)) {
                res.add(stringObjectIEdge);
            }
        }
        Assert.assertEquals(res.size(), 2);

        filter = new EdgeTsFilter(TimeRange.of(5L, 6L)).or(filter);
        res.clear();
        graphFilter = GraphFilter.of(filter);
        for (IEdge<String, Object> stringObjectIEdge : list) {
            if (graphFilter.filterEdge(stringObjectIEdge)) {
                res.add(stringObjectIEdge);
            }
        }
        Assert.assertEquals(res.size(), 3);

        graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.filterVertex(new IDVertex("1")));
        Assert.assertTrue(graphFilter.filterOneDegreeGraph(new OneDegreeGraph<>("1", null, null)));
    }
}
