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

package com.antgroup.geaflow.dsl.common.binary;

import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.checkResult;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getEdge;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getEdgeType;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getIdTypes;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getRow;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getRowType;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getVertex;
import static com.antgroup.geaflow.dsl.common.util.TestSchemaUtil.getVertexType;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.binary.decoder.IBinaryDecoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.IBinaryEncoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import org.testng.annotations.Test;

public class BinaryEncodeTest {

    @Test
    public void testEncodeVertex() {
        IType<?>[] idTypes = getIdTypes();
        for (IType<?> idType : idTypes) {
            VertexType vertexType = getVertexType(idType);
            RowVertex originVertex = getVertex(idType);
            IBinaryEncoder vertexEncoder = EncoderFactory.createEncoder(vertexType);
            Row encodeVertex = vertexEncoder.encode(originVertex);
            IBinaryDecoder vertexDecoder = DecoderFactory.createDecoder(vertexType);
            Row decodeVertex = vertexDecoder.decode(encodeVertex);
            checkResult(originVertex, decodeVertex, vertexType);
        }
    }

    @Test
    public void testEncodeEdge() {
        IType<?>[] idTypes = getIdTypes();
        for (IType<?> idType : idTypes) {
            EdgeType edgeType = getEdgeType(idType, true);
            RowEdge originEdge = getEdge(idType, true);
            IBinaryEncoder edgeEncoder = EncoderFactory.createEncoder(edgeType);
            Row encodeEdge = edgeEncoder.encode(originEdge);
            IBinaryDecoder edgeDecoder = DecoderFactory.createDecoder(edgeType);
            Row decodeEdge = edgeDecoder.decode(encodeEdge);
            checkResult(originEdge, decodeEdge, edgeType);
        }

        for (IType<?> idType : idTypes) {
            EdgeType edgeType = getEdgeType(idType, false);
            RowEdge originEdge = getEdge(idType, false);
            IBinaryEncoder edgeEncoder = EncoderFactory.createEncoder(edgeType);
            Row encodeEdge = edgeEncoder.encode(originEdge);
            IBinaryDecoder edgeDecoder = DecoderFactory.createDecoder(edgeType);
            Row decodeEdge = edgeDecoder.decode(encodeEdge);
            checkResult(originEdge, decodeEdge, edgeType);
        }
    }

    @Test
    public void testEncodeRow() {
        StructType rowType = getRowType();
        Row originRow = getRow();
        IBinaryEncoder rowEncoder = EncoderFactory.createEncoder(rowType);
        Row encodeRow = rowEncoder.encode(originRow);
        IBinaryDecoder rowDecoder = DecoderFactory.createDecoder(rowType);
        Row decodeRow = rowDecoder.decode(encodeRow);
        checkResult(originRow, decodeRow, rowType);
    }
}
