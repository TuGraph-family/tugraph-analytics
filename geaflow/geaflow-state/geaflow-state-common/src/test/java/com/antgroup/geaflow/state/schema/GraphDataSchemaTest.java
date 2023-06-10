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

package com.antgroup.geaflow.state.schema;

import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelEdge;
import com.antgroup.geaflow.model.graph.meta.GraphElementMetas;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.property.IPropertySerializable;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueTimeVertex;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.graph.encoder.VertexAtom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphDataSchemaTest {

    @Test
    public void test() throws Exception {
        GraphMeta graphMeta = new GraphMeta(new GraphMetaType(Types.STRING,
            ValueTimeVertex.class, ValueTimeVertex::new, String.class,
            ValueLabelEdge.class, ValueLabelEdge::new, ClassProperty.class));

        GraphDataSchema graphDataSchema = new GraphDataSchema(graphMeta);
        Assert.assertEquals(graphDataSchema.getKeyType(), Types.STRING);
        Assert.assertFalse(graphDataSchema.isEmptyEdgeProperty());
        Assert.assertFalse(graphDataSchema.isEmptyVertexProperty());

        IVertex vertex = graphDataSchema.getVertexConsFun().get();
        Assert.assertEquals(vertex.getClass(), graphMeta.getVertexMeta().getGraphElementClass());
        IEdge edge = graphDataSchema.getEdgeConsFun().get();
        Assert.assertEquals(edge.getClass(), graphMeta.getEdgeMeta().getGraphElementClass());

        Assert.assertTrue(graphDataSchema.getVertexAtoms().contains(VertexAtom.TIME));
        Assert.assertTrue(graphDataSchema.getEdgeAtoms().contains(EdgeAtom.LABEL));

        byte[] bytes = StringType.INSTANCE.serialize("foobar");
        Assert.assertEquals(graphDataSchema.getVertexPropertySerFun().apply("foobar"), bytes);
        Assert.assertEquals(graphDataSchema.getVertexPropertyDeFun().apply(bytes), "foobar");

        Assert.assertEquals(graphDataSchema.getEdgePropertySerFun().apply(new ClassProperty()), new byte[0]);
        Assert.assertEquals(graphDataSchema.getEdgePropertyDeFun().apply(new byte[0]), property);

        GraphElementMetas.clearCache();
        graphMeta = new GraphMeta(new GraphMetaType(Types.STRING,
            ValueTimeVertex.class, ValueTimeVertex::new, ClassProperty.class,
            ValueLabelEdge.class, ValueLabelEdge::new, String.class));
        graphDataSchema = new GraphDataSchema(graphMeta);

        bytes = StringType.INSTANCE.serialize("foobar");
        Assert.assertEquals(graphDataSchema.getEdgePropertySerFun().apply("foobar"), bytes);
        Assert.assertEquals(graphDataSchema.getEdgePropertyDeFun().apply(bytes), "foobar");

        Assert.assertEquals(graphDataSchema.getVertexPropertySerFun().apply(new ClassProperty()), new byte[0]);
        Assert.assertEquals(graphDataSchema.getVertexPropertyDeFun().apply(new byte[0]), property);
    }

    private static final ClassProperty property = new ClassProperty();

    public static final class ClassProperty implements IPropertySerializable {

        @Override
        public IPropertySerializable fromBinary(byte[] bytes) {
            return property;
        }

        @Override
        public byte[] toBytes() {
            return new byte[0];
        }

        @Override
        public IPropertySerializable clone() {
            return property;
        }
    }
}
