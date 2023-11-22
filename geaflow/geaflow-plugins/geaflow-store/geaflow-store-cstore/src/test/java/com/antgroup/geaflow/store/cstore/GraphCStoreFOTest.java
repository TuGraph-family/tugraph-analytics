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
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphCStoreFOTest {

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/CStoreFOTest"));
    }

    private GraphCStore<String, String, String> build(Map<String, String> config) {
        IStoreBuilder builder = StoreBuilderFactory.build("cstore");
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "CStoreFOTest");
        Configuration conf = new Configuration(config);
        StoreContext context = new StoreContext("cstore").withConfig(conf);
        context.withDataSchema(new GraphDataSchema(new GraphMeta(
            new GraphMetaType(StringType.INSTANCE,
                ValueLabelTimeVertex.class, ValueLabelTimeVertex::new, String.class,
                ValueLabelTimeEdge.class, ValueLabelTimeEdge::new, String.class))));
        GraphCStore<String, String, String> store = (GraphCStore) builder.getStore(DataModel.STATIC_GRAPH, conf);
        store.init(context);
        return store;
    }

    public void addData(GraphCStore<String, String, String> store) {
        for (int i = 0; i < 100; i++) {
            ValueLabelTimeVertex<String, String> vertex = new ValueLabelTimeVertex<>("a" + i, "b" + i, "foo", i);
            store.addVertex(vertex);
            ValueLabelTimeEdge<String, String> edge = new ValueLabelTimeEdge<>(
                "a" + i, "d", "b" + i, i % 2 == 0 ? EdgeDirection.OUT : EdgeDirection.IN, "foo", i);
            store.addEdge(edge);
        }
        store.flush();
    }

    public void testData(GraphCStore<String, String, String> store) {
        int count = 0;
        try (com.antgroup.geaflow.common.iterator.CloseableIterator<OneDegreeGraph<String,
            String, String>> iterator = store.getOneDegreeGraphIterator(StatePushDown.of())) {
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

    @Test
    public void testLocal() {
        Map<String, String> config = new HashMap<>();
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "Local");
        GraphCStore<String, String, String> store = build(config);
        addData(store);
        store.archive(1);
        store.close();
        FileUtils.deleteQuietly(new File("/tmp/CStoreFOTest"));
        store = build(config);
        store.recovery(1);
        testData(store);
        store.close();
        FileUtils.deleteQuietly(new File("/tmp/CStoreFOTest"));
    }
}
