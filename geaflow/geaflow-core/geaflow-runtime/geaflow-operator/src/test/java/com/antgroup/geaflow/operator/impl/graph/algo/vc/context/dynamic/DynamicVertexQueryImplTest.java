/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic;


import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction.VertexQuery;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.IntegerType;

import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.pushdown.filter.IVertexFilter;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DynamicVertexQueryImplTest {

    private VertexQuery<Integer, Integer> vertexQuery;
    private GraphState<Integer, Integer, Integer> graphState;

    @BeforeClass
    public void setup() {
        GraphStateDescriptor<Integer, Integer, Integer> desc =
            GraphStateDescriptor.build("test", StoreType.MEMORY.name());
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);

        GraphMetaType graphMetaType = new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
            Integer.class, ValueEdge.class, Integer.class);

        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView("test").withBackend(
            BackendType.RocksDB).withSchema(graphMetaType).withShardNum(1).build();
        desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        desc.withKeyGroup(new KeyGroup(0, 1023)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1024));

        graphState = StateFactory.buildGraphState(desc,
            new Configuration());

        graphState.dynamicGraph().V().add(0, new ValueVertex<>(1, 1));
        graphState.dynamicGraph().V().add(0, new ValueVertex<>(2, 2));
        graphState.dynamicGraph().V().add(0, new ValueVertex<>(3, 3));
        graphState.dynamicGraph().V().add(0, new ValueVertex<>(4, 4));
        graphState.dynamicGraph().V().add(0, new ValueVertex<>(5, 5));

        vertexQuery = new DynamicVertexQueryImpl<>(1, 0, graphState);

    }

    @Test
    public void testWithId() {
        vertexQuery.withId(2);
        IVertex<Integer, Integer> vertex = vertexQuery.get();
        int k = vertex.getId();
        int v = vertex.getValue();
        Assert.assertEquals(k, 2);
        Assert.assertEquals(v, 2);
    }

    @Test
    public void testGet() {
        vertexQuery = new DynamicVertexQueryImpl<>(1, 0, graphState);
        IVertex<Integer, Integer> vertex = vertexQuery.get();
        int k = vertex.getId();
        int v = vertex.getValue();
        Assert.assertEquals(k, 1);
        Assert.assertEquals(v, 1);

    }

    @Test
    public void testTestGet() {
        vertexQuery.withId(3);
        IVertex<Integer,Integer> vertex = vertexQuery.get(new IVertexFilter<Integer, Integer>() {
            @Override
            public boolean filter(IVertex<Integer, Integer> value) {
                return value.getValue() == 3;
            }
        });
        int k = vertex.getId();
        int v = vertex.getValue();
        Assert.assertEquals(k, 3);
        Assert.assertEquals(v,3);
    }
}
