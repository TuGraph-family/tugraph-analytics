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

package com.antgroup.geaflow.dsl.runtime.engine.source;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.binary.EncoderFactory;
import com.antgroup.geaflow.dsl.common.binary.encoder.VertexEncoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

@Deprecated
public class FileVertexTableSource extends AbstractFileTableSource<RowVertex> {

    private final Map<String, VertexType> vertexTypes;
    private Map<String, VertexEncoder> vertexEncoders;

    public FileVertexTableSource(Configuration config, Map<String, VertexType> vertexTypes) {
        super(replaceKey(config, ConnectorConfigKeys.GEAFLOW_DSL_USING_VERTEX_PATH,
            ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH));
        this.vertexTypes = Objects.requireNonNull(vertexTypes);
        this.vertexEncoders = new HashMap<>();
        for (Entry<String, VertexType> entry : vertexTypes.entrySet()) {
            vertexEncoders.put(entry.getKey(),
                EncoderFactory.createVertexEncoder(entry.getValue()));
        }
    }

    @Override
    protected List<RowVertex> convert(String[] fields) {
        Preconditions.checkArgument(VertexType.LABEL_FIELD_POSITION < fields.length,
            "Missing label field at position: " + VertexType.LABEL_FIELD_POSITION);
        String vertexLabel = fields[VertexType.LABEL_FIELD_POSITION];

        VertexType vertexType = vertexTypes.get(vertexLabel);
        Preconditions.checkArgument(vertexType != null,
            "Vertex label: " + vertexLabel + " is not defined in the schema.");
        Preconditions.checkArgument(vertexType.size() == fields.length,
            "Data fields size: " + fields.length
                + " is not equal to the schema fields size: " + vertexType.size()
                + " for vertex table: " + vertexLabel);

        RowVertex vertex = VertexEdgeFactory.createVertex(vertexType);
        Object id = TypeCastUtil.cast(fields[VertexType.ID_FIELD_POSITION], vertexType.getId().getType());
        vertex.setId(id);
        vertex.setBinaryLabel((BinaryString) BinaryUtil.toBinaryForString(vertexLabel));

        Object[] values = new Object[vertexType.getValueSize()];
        for (int i = vertexType.getValueOffset(); i < vertexType.size(); i++) {
            values[i - vertexType.getValueOffset()] = TypeCastUtil.cast(fields[i],
                vertexType.getType(i));
        }
        Row value = ObjectRow.create(values);
        vertex.setValue(value);
        vertex = vertexEncoders.get(vertexLabel).encode(vertex);
        return Lists.newArrayList(vertex);
    }
}
