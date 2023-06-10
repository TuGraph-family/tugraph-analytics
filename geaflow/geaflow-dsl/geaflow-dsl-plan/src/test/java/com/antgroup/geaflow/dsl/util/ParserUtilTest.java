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

package com.antgroup.geaflow.dsl.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.testng.annotations.Test;

public class ParserUtilTest {

    @Test
    public void testConvertTypeUtil() {
        RelDataType relType;
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();
        TableField field1 = new TableField("name", Types.STRING,true);
        TableField field2 = new TableField("id", Types.LONG,true);
        TableField field3 = new TableField("age", Types.DOUBLE,true);
        StructType structType =
            new StructType(Lists.newArrayList(field1, field2));
        assertEquals(structType.getField(1).getName(), "id");
        assertEquals(structType.getTypeClass(), Row.class);
        assertEquals(structType.indexOf("name"), 0);
        assertEquals(structType.getName(), "STRUCT");
        assertEquals(structType.getField("name").getName(), "name");
        structType.addField(field3);
        assertEquals(structType.getFieldNames().size(), 2);
        assertNotNull(structType.toString());
        structType.replace("name", field1);
        assertEquals(structType.getFieldNames().size(), 2);
        relType = SqlTypeUtil.convertToRelType(structType, true, typeFactory);
        assertEquals(relType.toString(), "RecordType:peek(VARCHAR name, BIGINT id)");
        assertEquals(SqlTypeUtil.convertToJavaTypes(relType, typeFactory).size(), 2);

        VertexType vertexType = new VertexType(Lists.newArrayList(field1, field2));
        assertEquals(vertexType.getField(1).getName(), "id");
        assertEquals(vertexType.getTypeClass(), RowVertex.class);
        assertEquals(vertexType.indexOf("name"), 0);
        assertEquals(vertexType.getName(), "VERTEX");
        assertEquals(vertexType.getField("name").getName(), "name");
        vertexType.addField(field3);
        assertEquals(vertexType.getFieldNames().size(), 2);
        assertNotNull(vertexType.toString());
        relType = SqlTypeUtil.convertToRelType(vertexType, true, typeFactory);
        assertEquals(relType.toString(),
            "Vertex:RecordType:peek(VARCHAR name, VARCHAR ~label, BIGINT id)");
        assertEquals(SqlTypeUtil.convertToJavaTypes(relType, typeFactory).size(), 3);
        assertEquals(SqlTypeUtil.convertType(relType).getName(), "VERTEX");

        EdgeType edgeType = new EdgeType(Lists.newArrayList(field1, field2), false);
        assertEquals(edgeType.getField(1).getName(), "id");
        assertEquals(edgeType.getTypeClass(), RowEdge.class);
        assertEquals(edgeType.indexOf("name"), 0);
        assertEquals(edgeType.getName(), "EDGE");
        assertEquals(edgeType.getField("name").getName(), "name");
        edgeType.addField(field3);
        assertEquals(edgeType.getFieldNames().size(), 2);
        assertNotNull(edgeType.toString());
        relType = SqlTypeUtil.convertToRelType(edgeType, true, typeFactory);
        assertEquals(relType.toString(),
            "Edge: RecordType:peek(VARCHAR name, BIGINT id, VARCHAR ~label)");
        assertEquals(SqlTypeUtil.convertToJavaTypes(relType, typeFactory).size(), 3);
        assertEquals(SqlTypeUtil.convertType(relType).getName(), "EDGE");

        PathType pathType = new PathType(Lists.newArrayList(field1, field2));
        assertEquals(pathType.getField(1).getName(), "id");
        assertEquals(pathType.getTypeClass(), Path.class);
        assertEquals(pathType.indexOf("name"), 0);
        assertEquals(pathType.getName(), "PATH");
        assertEquals(pathType.getField("name").getName(), "name");
        pathType.addField(field3);
        assertEquals(pathType.getFieldNames().size(), 2);
        assertNotNull(pathType.toString());
        pathType.replace("name", field1);
        assertEquals(pathType.getFieldNames().size(), 2);
        relType = SqlTypeUtil.convertToRelType(pathType, true, typeFactory);
        assertEquals(relType.toString(),
            "Path:RecordType:peek(VARCHAR name, BIGINT id)");
        assertEquals(SqlTypeUtil.convertToJavaTypes(relType, typeFactory).size(), 2);
        assertNotNull(SqlTypeUtil.convertType(relType));

        ArrayType arrayType = new ArrayType(Types.LONG);
        assertTrue(arrayType.getComponentType() instanceof LongType);
        assertEquals(arrayType.getTypeClass(), Long[].class);
        assertEquals(arrayType.getName(), "ARRAY");
        assertNotNull(arrayType.toString());
        Long [] longArray1 = new Long[] {0L, 1L, 2L};
        Long [] longArray2 = new Long[] {2L, 1L, 0L};
        assertEquals(arrayType.compare(longArray1, longArray2), -1);
        relType = SqlTypeUtil.convertToRelType(arrayType, true, typeFactory);
        assertEquals(relType.toString(), "BIGINT ARRAY");
        assertEquals(SqlTypeUtil.convertType(relType).getName(), "ARRAY");
    }

    @Test
    public void testEdgeDirection() {
        assertEquals(EdgeDirection.of("OUT").toString(), "OUT");
        assertEquals(EdgeDirection.of("IN").toString(), "IN");
        assertEquals(EdgeDirection.of("BOTH").toString(), "BOTH");
    }
}
