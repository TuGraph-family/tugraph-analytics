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

package com.antgroup.geaflow.dsl.common.binary.encoder;

import com.antgroup.geaflow.dsl.common.binary.EncoderFactory;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;

public class DefaultEdgeEncoder implements EdgeEncoder {

    private final RowEncoder rowEncoder;
    private final EdgeType edgeType;

    public DefaultEdgeEncoder(EdgeType edgeType) {
        StructType rowType = new StructType(edgeType.getValueFields());
        this.rowEncoder = EncoderFactory.createRowEncoder(rowType);
        this.edgeType = edgeType;
    }

    @Override
    public RowEdge encode(RowEdge rowEdge) {
        RowEdge binaryEdge = VertexEdgeFactory.createEdge(edgeType);
        binaryEdge.setSrcId(BinaryUtil.toBinaryForString(rowEdge.getSrcId()));
        binaryEdge.setTargetId(BinaryUtil.toBinaryForString(rowEdge.getTargetId()));
        if (edgeType.getTimestamp().isPresent()) {
            ((IGraphElementWithTimeField) binaryEdge).setTime(((IGraphElementWithTimeField) rowEdge).getTime());
        }
        binaryEdge.setDirect(rowEdge.getDirect());
        binaryEdge.setBinaryLabel(rowEdge.getBinaryLabel());
        binaryEdge.setValue(rowEncoder.encode(rowEdge.getValue()));
        return binaryEdge;
    }
}
