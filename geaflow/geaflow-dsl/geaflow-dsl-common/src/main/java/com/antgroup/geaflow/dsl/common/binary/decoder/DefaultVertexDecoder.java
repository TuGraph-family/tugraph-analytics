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
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import java.util.List;

public class DefaultVertexDecoder implements VertexDecoder {

    private final RowDecoder rowDecoder;
    private final VertexType vertexType;

    public DefaultVertexDecoder(VertexType vertexType) {
        StructType rowType = new StructType(vertexType.getValueFields());
        this.rowDecoder = DecoderFactory.createRowDecoder(rowType);
        this.vertexType = vertexType;
    }

    @Override
    public RowVertex decode(RowVertex rowVertex) {
        RowVertex decodeVertex = VertexEdgeFactory.createVertex(vertexType);
        decodeVertex.setId(rowVertex.getId());
        decodeVertex.setBinaryLabel(rowVertex.getBinaryLabel());
        Object[] values = new Object[vertexType.getValueSize()];
        List<TableField> valueFields = vertexType.getValueFields();
        for (int i = 0; i < valueFields.size(); i++) {
            values[i] = rowVertex.getField(vertexType.getValueOffset() + i,
                valueFields.get(i).getType());
        }
        decodeVertex.setValue(rowDecoder.decode(ObjectRow.create(values)));
        return decodeVertex;
    }
}
