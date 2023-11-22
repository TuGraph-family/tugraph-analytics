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
package com.antgroup.geaflow.store.cstore;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.AndFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeValueDropFilter;
import com.antgroup.geaflow.state.pushdown.filter.EmptyFilter;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OrFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexMustContainFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexValueDropFilter;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CStoreBuilderTest {

    private Configuration config;
    private IStoreBuilder builder;
    private StoreContext context;

    @BeforeClass
    public void setUp() {
        builder = StoreBuilderFactory.build("cstore");
        config = new Configuration(new HashMap<>());
        context = new StoreContext("cstore").withConfig(config);
        context.withDataSchema(new GraphDataSchema(new GraphMeta(
            new GraphMetaType(StringType.INSTANCE,
                ValueLabelTimeVertex.class, ValueLabelTimeVertex::new, String.class,
                ValueLabelTimeEdge.class, ValueLabelTimeEdge::new, String.class))));
    }

    @Test
    public void testStaticGraph() {
        GraphCStore store = (GraphCStore) builder.getStore(DataModel.STATIC_GRAPH, config);
        store.init(context);

        for (int i = 0; i < 100; i++) {
            ValueLabelTimeVertex<String, String> vertex = new ValueLabelTimeVertex<>("a" + i, "b" + i, "foo", i);
            store.addVertex(vertex);
            ValueLabelTimeEdge<String, String> edge = new ValueLabelTimeEdge<>(
                "a" + i, "d", "b" + i, i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN, "foo", i);
            store.addEdge(edge);
        }
        store.flush();

        testPointGetWithoutPushDown(store);
        testMultiPointGetWithoutPushDown(store);
        testScanWithoutPushDown(store);

        for (int i = 100; i < 200; i++) {
            ValueLabelTimeVertex<String, String> vertex = new ValueLabelTimeVertex<>("a" + i,
                "b" + i, "foo" + i, i);
            store.addVertex(vertex);
            ValueLabelTimeEdge<String, String> edge = new ValueLabelTimeEdge<>("a" + i, "d",
                "b" + i, i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN, "foo" + i, i);
            store.addEdge(edge);
        }
        store.flush();

        // vertex filter
        List<FilterType> vertexFilters = Lists.newArrayList(FilterType.EMPTY, FilterType.VERTEX_TS,
            FilterType.VERTEX_VALUE_DROP, FilterType.VERTEX_LABEL, FilterType.OR, FilterType.AND);
        for (FilterType filter : vertexFilters) {
            testPointGetVertexWithPushDown(store, filter);
            testMultiPointGetVertexWithPushDown(store, filter);
            testScanVertexWithPushDown(store, filter);
        }

        // edge filter
        List<FilterType> edgeFilters = Lists.newArrayList(FilterType.EMPTY, FilterType.IN_EDGE,
            FilterType.OUT_EDGE, FilterType.EDGE_TS, FilterType.EDGE_VALUE_DROP,
            FilterType.EDGE_LABEL, FilterType.OR, FilterType.AND);
        for (FilterType filter : edgeFilters) {
            testPointGetEdgesWithPushDown(store, filter);
            testMultiPointGetEdgesWithPushDown(store, filter);
            testScanEdgeWithPushDown(store, filter);
        }

        // oneDegreeGraph filter
        List<FilterType> oneDegreeGraphFilters = Lists.newArrayList(FilterType.EMPTY,
            FilterType.IN_EDGE, FilterType.OUT_EDGE, FilterType.EDGE_TS, FilterType.OR,
            FilterType.AND, FilterType.VERTEX_TS, FilterType.EDGE_VALUE_DROP,
            FilterType.VERTEX_VALUE_DROP, FilterType.EDGE_LABEL, FilterType.VERTEX_LABEL,
            FilterType.VERTEX_MUST_CONTAIN);
        for (FilterType filterType : oneDegreeGraphFilters) {
            testPointGetOneDegreeGraphWithPushDown(store, filterType);
            testMultiPointGetOneDegreeGraphWithPushDown(store, filterType);
            testScanOneDegreeGraphWithPushDown(store, filterType);
        }
    }

    private void testPointGetWithoutPushDown(GraphCStore store) {
        for (int i = 0; i < 100; i++) {
            String key = "a" + i;
            ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) store.getVertex(key, StatePushDown.of());
            Assert.assertEquals(vertex.getLabel(), "foo");
            Assert.assertEquals(vertex.getTime(), i);
            Assert.assertEquals(vertex.getValue(), "b" + i);

            List<IEdge> edges = store.getEdges(key, StatePushDown.of());
            Assert.assertEquals(edges.size(), 1);
            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

            Assert.assertEquals(edge.getLabel(), "foo");
            Assert.assertEquals(edge.getTime(), i);
            Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
            Assert.assertEquals(edge.getTargetId(), "d");
            Assert.assertEquals(edge.getValue(), "b" + i);

            OneDegreeGraph oneDegree = store.getOneDegreeGraph(key, StatePushDown.of());
            Assert.assertEquals(oneDegree.getKey(), key);
            vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
            Assert.assertEquals(vertex.getLabel(), "foo");
            Assert.assertEquals(vertex.getTime(), i);
            Assert.assertEquals(vertex.getValue(), "b" + i);

            edges = Lists.newArrayList(oneDegree.getEdgeIterator());
            Assert.assertEquals(edges.size(), 1);
            edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

            Assert.assertEquals(edge.getLabel(), "foo");
            Assert.assertEquals(edge.getTime(), i);
            Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
            Assert.assertEquals(edge.getTargetId(), "d");
            Assert.assertEquals(edge.getValue(), "b" + i);
        }
    }

    private void testMultiPointGetWithoutPushDown(GraphCStore store) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add("a" + i);
        }

        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IVertex> vertexIterator
            = store.getVertexIterator(keys, StatePushDown.of())) {
            while (vertexIterator.hasNext()) {
                count++;
                ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                int idx = (int) vertex.getTime();
                Assert.assertEquals(vertex.getLabel(), "foo");
                Assert.assertEquals(vertex.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IEdge> edgeIterator =
            store.getEdgeIterator(keys, StatePushDown.of())) {
            while (edgeIterator.hasNext()) {
                count++;
                ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                int idx = (int) edge.getTime();
                Assert.assertEquals(edge.getLabel(), "foo");
                Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                Assert.assertEquals(edge.getTargetId(), "d");
                Assert.assertEquals(edge.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<OneDegreeGraph> iterator =
            store.getOneDegreeGraphIterator(keys, StatePushDown.of())) {
            while (iterator.hasNext()) {
                count++;
                OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                Assert.assertEquals(vertex.getLabel(), "foo");
                Assert.assertEquals(vertex.getTime(), idx);
                Assert.assertEquals(vertex.getValue(), "b" + idx);

                List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                Assert.assertEquals(edges.size(), 1);
                ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                Assert.assertEquals(edge.getLabel(), "foo");
                Assert.assertEquals(edge.getTime(), idx);
                Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                Assert.assertEquals(edge.getTargetId(), "d");
                Assert.assertEquals(edge.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);
    }

    private void testScanWithoutPushDown(GraphCStore store) {
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IVertex> vertexIterator
            = store.getVertexIterator(StatePushDown.of())) {
            while (vertexIterator.hasNext()) {
                count++;
                ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                int idx = (int) vertex.getTime();
                Assert.assertEquals(vertex.getLabel(), "foo");
                Assert.assertEquals(vertex.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IEdge> edgeIterator =
            store.getEdgeIterator(StatePushDown.of())) {
            while (edgeIterator.hasNext()) {
                count++;
                ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                int idx = (int) edge.getTime();
                Assert.assertEquals(edge.getLabel(), "foo");
                Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                Assert.assertEquals(edge.getTargetId(), "d");
                Assert.assertEquals(edge.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);

        count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<OneDegreeGraph> iterator =
            store.getOneDegreeGraphIterator(StatePushDown.of())) {
            while (iterator.hasNext()) {
                count++;
                OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                Assert.assertEquals(vertex.getLabel(), "foo");
                Assert.assertEquals(vertex.getTime(), idx);
                Assert.assertEquals(vertex.getValue(), "b" + idx);

                List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                Assert.assertEquals(edges.size(), 1);
                ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                Assert.assertEquals(edge.getLabel(), "foo");
                Assert.assertEquals(edge.getTime(), idx);
                Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                Assert.assertEquals(edge.getTargetId(), "d");
                Assert.assertEquals(edge.getValue(), "b" + idx);
            }
        }
        Assert.assertEquals(count, 100);
    }

    private void testPointGetVertexWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, true);
        for (int i = 0; i < 200; i++) {
            String key = "a" + i;
            ValueLabelTimeVertex<String, String> vertex =
                (ValueLabelTimeVertex<String, String>) store.getVertex(key, pushDown);
            switch (filterType) {
                case EMPTY:
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);
                    break;
                case OR:
                case VERTEX_LABEL:
                    if (i < 100) {
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getTime(), i);
                        Assert.assertEquals(vertex.getValue(), "b" + i);
                    } else {
                        Assert.assertNull(vertex);
                    }
                    break;
                case AND:
                case VERTEX_TS:
                    if (i < 10) {
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getTime(), i);
                        Assert.assertEquals(vertex.getValue(), "b" + i);
                    } else {
                        Assert.assertNull(vertex);
                    }
                    break;
                case VERTEX_VALUE_DROP:
                    Assert.assertEquals(vertex.getLabel(), "foo" + (i < 100 ? "" : i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertNull(vertex.getValue());
                    break;
                case IN_EDGE:
                case OUT_EDGE:
                case EDGE_TS:
                case EDGE_VALUE_DROP:
                case EDGE_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testPointGetEdgesWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, false);
        for (int i = 0; i < 200; i++) {
            String key = "a" + i;
            List<IEdge> edges = store.getEdges(key, pushDown);
            ValueLabelTimeEdge<String, String> edge;
            switch (filterType) {
                case EMPTY:
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertEquals(edge.getValue(), "b" + i);
                    break;
                case IN_EDGE:
                    if (i % 2 == 1) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                        Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case OUT_EDGE:
                    if (i % 2 == 0) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                        Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.OUT);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case AND:
                case EDGE_TS:
                    if (i < 10) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                        Assert.assertTrue(edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case OR:
                case EDGE_LABEL:
                    if (i < 100) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                        Assert.assertTrue(edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case EDGE_VALUE_DROP:
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);
                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertNull(edge.getValue());
                    break;
                case VERTEX_TS:
                case VERTEX_VALUE_DROP:
                case VERTEX_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testPointGetOneDegreeGraphWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, true);
        for (int i = 0; i < 200; i++) {
            String key = "a" + i;
            OneDegreeGraph oneDegree = store.getOneDegreeGraph(key, pushDown);
            ValueLabelTimeVertex<String, String> vertex;
            List<IEdge> edges;
            ValueLabelTimeEdge<String, String> edge;
            switch (filterType) {
                case EMPTY:
                case VERTEX_MUST_CONTAIN:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertEquals(edge.getValue(), "b" + i);
                    break;
                case IN_EDGE:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    if (i % 2 == 1) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case OUT_EDGE:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    if (i % 2 == 0) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.OUT);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case EDGE_TS:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    if (i < 10) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case AND:
                case VERTEX_TS:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    if (i < 10) {
                        Assert.assertTrue(vertex.getLabel().equals("foo"));
                        Assert.assertEquals(vertex.getTime(), i);
                        Assert.assertEquals(vertex.getValue(), "b" + i);
                    } else {
                        Assert.assertNull(vertex);
                    }

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                    Assert.assertEquals(edge.getLabel(), "foo" + (i < 100 ? "" : i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertEquals(edge.getValue(), "b" + i);
                    break;
                case EDGE_VALUE_DROP:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertNull(edge.getValue());
                    break;
                case VERTEX_VALUE_DROP:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertNull(vertex.getValue());

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertEquals(edge.getValue(), "b" + i);
                    break;
                case EDGE_LABEL:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                    Assert.assertEquals(vertex.getTime(), i);
                    Assert.assertEquals(vertex.getValue(), "b" + i);

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    if (i < 100) {
                        Assert.assertEquals(edges.size(), 1);
                        edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getTime(), i);
                        Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + i);
                    } else {
                        Assert.assertTrue(CollectionUtils.isEmpty(edges));
                    }
                    break;
                case OR:
                case VERTEX_LABEL:
                    Assert.assertEquals(oneDegree.getKey(), key);
                    vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                    if (i < 100) {
                        Assert.assertTrue(i < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + i));
                        Assert.assertEquals(vertex.getTime(), i);
                        Assert.assertEquals(vertex.getValue(), "b" + i);
                    } else {
                        Assert.assertNull(vertex);
                    }

                    edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                    Assert.assertEquals(edges.size(), 1);
                    edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                    Assert.assertTrue(i < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + i));
                    Assert.assertEquals(edge.getTime(), i);
                    Assert.assertEquals(edge.getDirect(), i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                    Assert.assertEquals(edge.getTargetId(), "d");
                    Assert.assertEquals(edge.getValue(), "b" + i);
                    break;
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }

        }
    }

    private void testMultiPointGetVertexWithPushDown(GraphCStore store, FilterType filterType) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("a" + i);
        }
        IStatePushDown pushDown = generateStatePushDown(filterType, keys, true);
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IVertex> vertexIterator
            = store.getVertexIterator(keys, pushDown)) {
            switch (filterType) {
                case EMPTY:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case AND:
                case VERTEX_TS:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 10);
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 10);
                    break;
                case VERTEX_VALUE_DROP:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertNull(vertex.getValue());
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case OR:
                case VERTEX_LABEL:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 100);
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case IN_EDGE:
                case OUT_EDGE:
                case EDGE_TS:
                case EDGE_VALUE_DROP:
                case EDGE_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testMultiPointGetEdgesWithPushDown(GraphCStore store, FilterType filterType) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("a" + i);
        }
        IStatePushDown pushDown = generateStatePushDown(filterType, keys, false);
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IEdge> edgeIterator =
            store.getEdgeIterator(keys, pushDown)) {
            switch (filterType) {
                case EMPTY:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case IN_EDGE:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx % 2 == 1);
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case OUT_EDGE:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx % 2 == 0);
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.OUT);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case AND:
                case EDGE_TS:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 10 && edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 10);
                    break;
                case OR:
                case EDGE_LABEL:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case EDGE_VALUE_DROP:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertNull(edge.getValue());
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case VERTEX_TS:
                case VERTEX_VALUE_DROP:
                case VERTEX_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testMultiPointGetOneDegreeGraphWithPushDown(GraphCStore store,
                                                          FilterType filterType) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("a" + i);
        }
        IStatePushDown pushDown = generateStatePushDown(filterType, keys, true);
        int vertexCount = 0;
        int edgeCount = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<OneDegreeGraph> iterator =
            store.getOneDegreeGraphIterator(keys, pushDown)) {
            switch (filterType) {
                case EMPTY:
                case VERTEX_MUST_CONTAIN:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case IN_EDGE:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx % 2 == 1) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case OUT_EDGE:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx % 2 == 0) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case EDGE_TS:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx < 10) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 10);
                    break;
                case AND:
                case VERTEX_TS:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        if (idx < 10) {
                            vertexCount++;
                            Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                            Assert.assertEquals(vertex.getTime(), idx);
                            Assert.assertEquals(vertex.getValue(), "b" + idx);
                        } else {
                            Assert.assertNull(vertex);
                        }

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case EDGE_VALUE_DROP:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertNull(edge.getValue());
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case VERTEX_VALUE_DROP:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertNull(vertex.getValue());

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case EDGE_LABEL:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx < 100) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case OR:
                case VERTEX_LABEL:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        if (idx < 100) {
                            vertexCount++;
                            Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                            Assert.assertEquals(vertex.getTime(), idx);
                            Assert.assertEquals(vertex.getValue(), "b" + idx);
                        } else {
                            Assert.assertNull(vertex);
                        }

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 100);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testScanVertexWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, true);
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IVertex> vertexIterator =
            store.getVertexIterator(
            pushDown)) {
            switch (filterType) {
                case EMPTY:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex =
                            (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(
                            idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel()
                                .equals("foo" + idx));
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case AND:
                case VERTEX_TS:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex =
                            (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 10);
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 10);
                    break;
                case VERTEX_VALUE_DROP:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex =
                            (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(
                            idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel()
                                .equals("foo" + idx));
                        Assert.assertNull(vertex.getValue());
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case OR:
                case VERTEX_LABEL:
                    while (vertexIterator.hasNext()) {
                        count++;
                        ValueLabelTimeVertex<String, String> vertex =
                            (ValueLabelTimeVertex<String, String>) vertexIterator.next();
                        int idx = (int) vertex.getTime();
                        Assert.assertTrue(idx < 100);
                        Assert.assertEquals(vertex.getLabel(), "foo");
                        Assert.assertEquals(vertex.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case IN_EDGE:
                case OUT_EDGE:
                case EDGE_TS:
                case EDGE_VALUE_DROP:
                case EDGE_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }

    }

    private void testScanEdgeWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, false);
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<IEdge> edgeIterator =
            store.getEdgeIterator(pushDown)) {
            switch (filterType) {
                case EMPTY:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case IN_EDGE:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx % 2 == 1);
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case OUT_EDGE:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx % 2 == 0);
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), EdgeDirection.OUT);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case AND:
                case EDGE_TS:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 10 && edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 10);
                    break;
                case OR:
                case EDGE_LABEL:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo"));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(count, 100);
                    break;
                case EDGE_VALUE_DROP:
                    while (edgeIterator.hasNext()) {
                        count++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edgeIterator.next();
                        int idx = (int) edge.getTime();
                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertNull(edge.getValue());
                    }
                    Assert.assertEquals(count, 200);
                    break;
                case VERTEX_TS:
                case VERTEX_VALUE_DROP:
                case VERTEX_LABEL:
                case VERTEX_MUST_CONTAIN:
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private void testScanOneDegreeGraphWithPushDown(GraphCStore store, FilterType filterType) {
        IStatePushDown pushDown = generateStatePushDown(filterType, null, true);
        int vertexCount = 0;
        int edgeCount = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<OneDegreeGraph> iterator =
            store.getOneDegreeGraphIterator(pushDown)) {
            switch (filterType) {
                case EMPTY:
                case VERTEX_MUST_CONTAIN:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case IN_EDGE:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx % 2 == 1) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case OUT_EDGE:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx % 2 == 0) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case EDGE_TS:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx < 10) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 10);
                    break;
                case AND:
                case VERTEX_TS:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        if (idx < 10) {
                            vertexCount++;
                            Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                            Assert.assertEquals(vertex.getTime(), idx);
                            Assert.assertEquals(vertex.getValue(), "b" + idx);
                        } else {
                            Assert.assertNull(vertex);
                        }

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 10);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case EDGE_VALUE_DROP:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertNull(edge.getValue());
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case VERTEX_VALUE_DROP:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertNull(vertex.getValue());

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case EDGE_LABEL:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        vertexCount++;
                        Assert.assertTrue(idx < 100 && vertex.getLabel().equals("foo") || vertex.getLabel().equals("foo" + idx));
                        Assert.assertEquals(vertex.getTime(), idx);
                        Assert.assertEquals(vertex.getValue(), "b" + idx);

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        if (idx < 100) {
                            Assert.assertEquals(edges.size(), 1);
                            edgeCount++;
                            ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                            Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                            Assert.assertEquals(edge.getTime(), idx);
                            Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                            Assert.assertEquals(edge.getTargetId(), "d");
                            Assert.assertEquals(edge.getValue(), "b" + idx);
                        } else {
                            Assert.assertTrue(CollectionUtils.isEmpty(edges));
                        }
                    }
                    Assert.assertEquals(vertexCount, 200);
                    Assert.assertEquals(edgeCount, 100);
                    break;
                case OR:
                case VERTEX_LABEL:
                    while (iterator.hasNext()) {
                        OneDegreeGraph<String, String, String> oneDegree = iterator.next();
                        int idx = Integer.parseInt(oneDegree.getKey().substring(1));
                        ValueLabelTimeVertex<String, String> vertex = (ValueLabelTimeVertex<String, String>) oneDegree.getVertex();
                        if (idx < 100) {
                            vertexCount++;
                            Assert.assertTrue(vertex.getLabel().equals("foo") || vertex.getLabel()
                                .equals("foo" + idx));
                            Assert.assertEquals(vertex.getTime(), idx);
                            Assert.assertEquals(vertex.getValue(), "b" + idx);
                        } else {
                            Assert.assertNull(vertex);
                        }

                        List<IEdge<String, String>> edges = Lists.newArrayList(oneDegree.getEdgeIterator());
                        Assert.assertEquals(edges.size(), 1);
                        edgeCount++;
                        ValueLabelTimeEdge<String, String> edge = (ValueLabelTimeEdge<String, String>) edges.get(0);

                        Assert.assertTrue(idx < 100 && edge.getLabel().equals("foo") || edge.getLabel().equals("foo" + idx));
                        Assert.assertEquals(edge.getTime(), idx);
                        Assert.assertEquals(edge.getDirect(), idx % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN);
                        Assert.assertEquals(edge.getTargetId(), "d");
                        Assert.assertEquals(edge.getValue(), "b" + idx);
                    }
                    Assert.assertEquals(vertexCount, 100);
                    Assert.assertEquals(edgeCount, 200);
                    break;
                case TTL:
                case ONLY_VERTEX:
                case GENERATED:
                case OTHER:
                default:
                    throw new RuntimeException("unsupported filter type " + filterType.name());
            }
        }
    }

    private IStatePushDown generateStatePushDown(FilterType filterType, List<String> keys,
                                                 boolean forVertex) {
        StatePushDown pushDown = StatePushDown.of();
        IFilter filter = generateFilter(filterType, forVertex);
        if (keys == null || keys.isEmpty()) {
            return pushDown.withFilter(filter);
        }
        Map<String, IFilter> filterMap = new HashMap<>();
        keys.forEach(key -> filterMap.put(key, filter));
        return pushDown.withFilters(filterMap);
    }

    private IFilter generateFilter(FilterType filterType, boolean forVertex) {
        List<IFilter> filters;
        switch (filterType) {
            case EMPTY:
                return EmptyFilter.of();
            case IN_EDGE:
                return InEdgeFilter.instance();
            case OUT_EDGE:
                return OutEdgeFilter.instance();
            case EDGE_TS:
                return EdgeTsFilter.instance(0L, 10L);
            case OR:
                filters = new ArrayList<>();
                if (forVertex) {
                    filters.add(generateFilter(FilterType.VERTEX_TS, forVertex));
                    filters.add(generateFilter(FilterType.VERTEX_LABEL, forVertex));
                } else {
                    filters.add(generateFilter(FilterType.EDGE_TS, forVertex));
                    filters.add(generateFilter(FilterType.EDGE_LABEL, forVertex));
                }
                return new OrFilter(filters);
            case AND:
                filters = new ArrayList<>();
                if (forVertex) {
                    filters.add(generateFilter(FilterType.VERTEX_TS, forVertex));
                    filters.add(generateFilter(FilterType.VERTEX_LABEL, forVertex));
                } else {
                    filters.add(generateFilter(FilterType.EDGE_TS, forVertex));
                    filters.add(generateFilter(FilterType.EDGE_LABEL, forVertex));
                }
                return new AndFilter(filters);
            case VERTEX_TS:
                return VertexTsFilter.instance(0L, 10L);
            case EDGE_VALUE_DROP:
                return EdgeValueDropFilter.instance();
            case VERTEX_VALUE_DROP:
                return VertexValueDropFilter.instance();
            case EDGE_LABEL:
                return EdgeLabelFilter.instance("foo");
            case VERTEX_LABEL:
                return VertexLabelFilter.instance("foo");
            case VERTEX_MUST_CONTAIN:
                return VertexMustContainFilter.instance();
            case TTL:
            case ONLY_VERTEX:
            case GENERATED:
            case OTHER:
            default:
                throw new RuntimeException("unsupported filter type " + filterType.name());
        }
    }
}