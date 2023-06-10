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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyListStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyMapStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StateFactoryTest {

    private long ts;
    private String testName;

    @BeforeMethod
    public void setUp() {
        this.ts = System.currentTimeMillis();
        this.testName = "factory-test-" + ts;
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteQuietly(new File("/tmp/" + testName));
    }

    @Test
    public void testGraphState() throws Exception {
        Map<String, String> config = new HashMap<>();

        GraphMetaType tag = new GraphMetaType(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class);
        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withGraphMeta(new GraphMeta(tag)).withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < 100; i++) {
            graphState.staticGraph().V().add(new ValueVertex<>("hello" + ts, "hello"));
            graphState.staticGraph().E().add(new ValueEdge<>("hello" + ts, "world" + i,
                "hello" + i));
        }
        graphState.staticGraph().V().add(new ValueVertex<>("world" + ts, "world"));

        Iterator<OneDegreeGraph<String, String, String>> it = graphState.staticGraph().VE().query(
            "hello" + ts, "world" + ts).iterator();
        Map<String, OneDegreeGraph<String, String, String>> res = new HashMap<>();
        while (it.hasNext()) {
            OneDegreeGraph<String, String, String> oneDegreeGraph = it.next();
            res.put(oneDegreeGraph.getKey(), oneDegreeGraph);
        }

        Assert.assertEquals(res.size(), 2);
        OneDegreeGraph<String, String, String> oneDegreeGraph = res.get("hello" + ts);
        Assert.assertEquals(oneDegreeGraph.getVertex().getValue(), "hello");
        Assert.assertEquals(Iterators.size(oneDegreeGraph.getEdgeIterator()), 100);
        oneDegreeGraph = res.get("world" + ts);
        Assert.assertEquals(oneDegreeGraph.getVertex().getValue(), "world");
        Assert.assertEquals(Iterators.size(oneDegreeGraph.getEdgeIterator()), 0);
    }

    @Test
    public void testKeyValueState() {
        Map<String, String> config = new HashMap<>();
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withDefaultValue(() -> "foobar").withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyValueState<String, String> valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        valueState.put("hello", "world");
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");
    }

    @Test
    public void testKeyListState() {
        Map<String, String> config = new HashMap<>();
        KeyListStateDescriptor<String, String> desc =
            KeyListStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));;
        KeyListState<String, String> listState = StateFactory.buildKeyListState(desc,
            new Configuration(config));

        listState.add("hello", "world");
        listState.put("foo", Arrays.asList("bar1", "bar2"));
        Assert.assertEquals(listState.get("hello"), Arrays.asList("world"));
        Assert.assertEquals(listState.get("foo"), Arrays.asList("bar1", "bar2"));
        listState.remove("foo");
        Assert.assertEquals(listState.get("foo").size(), 0);
    }

    @Test
    public void testKeyMapState() {
        Map<String, String> config = new HashMap<>();
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));;
        KeyMapState<String, String, String> mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));

        mapState.put("hello", config);
        mapState.add("foo", "bar1", "bar2");
        Assert.assertEquals(mapState.get("hello").size(), config.size());
        Assert.assertEquals(mapState.get("foo").get("bar1"), "bar2");
        mapState.remove("hello");
        Assert.assertEquals(mapState.get("hello").size(), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_0() {
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyValueState(desc, new Configuration());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_1() {
        KeyListStateDescriptor<String, String> desc =
            KeyListStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyListState(desc, new Configuration());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_2() {
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyMapState(desc, new Configuration());
    }

    @Test(expectedExceptions = Exception.class)
    public void testUnsupportedStore_3() {
        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildGraphState(desc, new Configuration());
    }
}
