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

package com.antgroup.geaflow.dsl.common.binary.decoder;

import com.antgroup.geaflow.dsl.common.binary.DecoderFactory;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import java.util.List;

public class DefaultEdgeDecoder implements EdgeDecoder {

    private final RowDecoder rowDecoder;
    private final EdgeType edgeType;

    public DefaultEdgeDecoder(EdgeType edgeType) {
        StructType rowType = new StructType(edgeType.getValueFields());
        this.rowDecoder = DecoderFactory.createRowDecoder(rowType);
        this.edgeType = edgeType;
    }

    @Override
    public RowEdge decode(RowEdge rowEdge) {
        RowEdge decodeEdge = VertexEdgeFactory.createEdge(edgeType);
        decodeEdge.setSrcId(rowEdge.getSrcId());
        decodeEdge.setTargetId(rowEdge.getTargetId());
        decodeEdge.setBinaryLabel(rowEdge.getBinaryLabel());
        decodeEdge.setDirect(rowEdge.getDirect());
        if (edgeType.getTimestamp().isPresent()) {
            ((IGraphElementWithTimeField) decodeEdge).setTime(((IGraphElementWithTimeField) rowEdge).getTime());
        }
        Object[] values = new Object[edgeType.getValueSize()];
        List<TableField> valueFields = edgeType.getValueFields();
        for (int i = 0; i < valueFields.size(); i++) {
            values[i] = rowEdge.getField(edgeType.getValueOffset() + i,
                valueFields.get(i).getType());
        }
        decodeEdge.setValue(rowDecoder.decode(ObjectRow.create(values)));
        return decodeEdge;
    }
}
