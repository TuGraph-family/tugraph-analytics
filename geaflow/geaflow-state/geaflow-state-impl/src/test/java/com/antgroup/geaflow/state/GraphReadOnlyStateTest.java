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
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.graph.StateMode;
import com.antgroup.geaflow.state.manage.LoadOption;
import com.antgroup.geaflow.state.pushdown.filter.IEdgeFilter;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphReadOnlyStateTest {

    private Map<String, String> config;

    @BeforeMethod
    public void setUp() throws IOException {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksDBGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
    }

    @Test
    public void testStaticReadOnlyState() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.staticGraph().E().add(new ValueEdge<>("1", "2", "hello", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("1", "3", "hello", EdgeDirection.OUT));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "2", "world", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "3", "world", EdgeDirection.OUT));
        graphState.staticGraph().V().add(new ValueVertex<>("1", "3"));
        graphState.staticGraph().V().add(new ValueVertex<>("2", "4"));
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        Iterator<IVertex<String, String>> iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();


        desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withStateMode(StateMode.RDONLY);

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().setCheckpointId(1);
        graphState.manage().operate().recover();

        list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().drop();
    }

    @Test
    public void testDynamicReadOnlyState() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test2", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.dynamicGraph().E().add(1, new ValueEdge<>("1", "2", "hello", EdgeDirection.IN));
        graphState.dynamicGraph().E().add(2, new ValueEdge<>("1", "3", "hello", EdgeDirection.OUT));
        graphState.dynamicGraph().E().add(1, new ValueEdge<>("2", "2", "world", EdgeDirection.IN));
        graphState.dynamicGraph().E().add(2, new ValueEdge<>("2", "3", "world", EdgeDirection.OUT));
        graphState.dynamicGraph().V().add(1, new ValueVertex<>("1", "3"));
        graphState.dynamicGraph().V().add(1, new ValueVertex<>("2", "4"));
        graphState.dynamicGraph().V().add(2, new ValueVertex<>("1", "5"));
        graphState.dynamicGraph().V().add(2, new ValueVertex<>("2", "6"));
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list =
            graphState.dynamicGraph().E().query(1L, Arrays.asList("1", "2")).asList();
        Assert.assertEquals(list.size(), 2);

        long version = graphState.dynamicGraph().V().getLatestVersion("2");
        Assert.assertEquals(version, 2);

        IVertex<String, String> vertex = graphState.dynamicGraph().V().query(2, "1").get();
        Assert.assertEquals(vertex.getValue(), "5");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();


        desc = GraphStateDescriptor.build("test2", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withDataModel(DataModel.DYNAMIC_GRAPH).withStateMode(StateMode.RDONLY);

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().setCheckpointId(1);
        graphState.manage().operate().recover();

        list = graphState.dynamicGraph().E().query(1L, Arrays.asList("1", "2")).asList();
        Assert.assertEquals(list.size(), 2);

        version = graphState.dynamicGraph().V().getLatestVersion("2");
        Assert.assertEquals(version, 2);

        vertex = graphState.dynamicGraph().V().query(2, "1").get();
        Assert.assertEquals(vertex.getValue(), "5");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test(invocationCount = 5)
    public void testReadOnlyStateWarmup() throws IOException {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));

        Configuration configuration = new Configuration(config);
        ViewMetaBookKeeper viewMetaBookKeeper = new ViewMetaBookKeeper("test1", configuration);
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, configuration);

        graphState.manage().operate().setCheckpointId(1);

        graphState.staticGraph().E().add(new ValueEdge<>("1", "2", "hello", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("1", "3", "hello", EdgeDirection.OUT));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "2", "world", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "3", "world", EdgeDirection.OUT));
        graphState.staticGraph().V().add(new ValueVertex<>("1", "3"));
        graphState.staticGraph().V().add(new ValueVertex<>("2", "4"));
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        Iterator<IVertex<String, String>> iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        viewMetaBookKeeper.saveViewVersion(1);
        viewMetaBookKeeper.archive();


        desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withStateMode(StateMode.RDONLY);

        config.put(StateConfigKeys.STATE_BACKGROUND_SYNC_ENABLE.getKey(), "true");
        config.put(StateConfigKeys.STATE_RECOVER_LATEST_VERSION_ENABLE.getKey(), "true");

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().load(LoadOption.of());

        list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().drop();
        viewMetaBookKeeper.archive();
    }
}