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
import com.antgroup.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

@Deprecated
public class FileEdgeTableSource extends AbstractFileTableSource<RowEdge> {

    private final Map<String, EdgeType> edgeTypes;
    private Map<String, EdgeEncoder> edgeEncoders;

    public FileEdgeTableSource(Configuration config, Map<String, EdgeType> edgeTypes) {
        super(replaceKey(config, ConnectorConfigKeys.GEAFLOW_DSL_USING_EDGE_PATH,
            ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH));
        this.edgeTypes = Objects.requireNonNull(edgeTypes);
        this.edgeEncoders = new HashMap<>();
        for (Entry<String, EdgeType> entry : edgeTypes.entrySet()) {
            edgeEncoders.put(entry.getKey(),
                EncoderFactory.createEdgeEncoder(entry.getValue()));
        }
    }

    @Override
    protected List<RowEdge> convert(String[] fields) {
        Preconditions.checkArgument(EdgeType.LABEL_FIELD_POSITION < fields.length,
            "Missing label field at position: " + EdgeType.LABEL_FIELD_POSITION);

        String edgeLabel = fields[EdgeType.LABEL_FIELD_POSITION];
        EdgeType edgeType = edgeTypes.get(edgeLabel);
        Preconditions.checkArgument(edgeType != null,
            "Edge label: " + edgeLabel + " is not defined in the schema.");
        Preconditions.checkArgument(edgeType.size() == fields.length,
            "Data fields size: " + fields.length
                + " is not equal to the schema fields size: " + edgeType.size()
                + " for vertex table: " + edgeLabel);

        RowEdge edge = VertexEdgeFactory.createEdge(edgeType);
        edge.setBinaryLabel((BinaryString) BinaryUtil.toBinaryForString(edgeLabel));
        Object srcId = TypeCastUtil.cast(fields[EdgeType.SRC_ID_FIELD_POSITION],
            edgeType.getSrcId().getType());
        edge.setSrcId(srcId);

        Object targetId = TypeCastUtil.cast(fields[EdgeType.TARGET_ID_FIELD_POSITION],
            edgeType.getTargetId().getType());
        edge.setTargetId(targetId);

        if (edgeType.getTimestamp().isPresent()) {
            long ts = (Long) TypeCastUtil.cast(fields[EdgeType.TIME_FIELD_POSITION],
                edgeType.getTimestamp().get().getType());
            ((IGraphElementWithTimeField) edge).setTime(ts);
        }

        Object[] values = new Object[edgeType.getValueSize()];
        for (int i = edgeType.getValueOffset(); i < edgeType.size(); i++) {
            values[i - edgeType.getValueOffset()] = TypeCastUtil.cast(fields[i],
                edgeType.getType(i));
        }
        Row value = ObjectRow.create(values);
        edge.setValue(value);
        edge = edgeEncoders.get(edgeLabel).encode(edge);

        EdgeDirection reverseDirection;
        switch (edge.getDirect()) {
            case IN:
                reverseDirection = EdgeDirection.OUT;
                break;
            case OUT:
                reverseDirection = EdgeDirection.IN;
                break;
            default:
                reverseDirection = edge.getDirect();
        }
        RowEdge otherDirectEdge = (RowEdge) edge.reverse();
        otherDirectEdge.setValue(value);
        otherDirectEdge = otherDirectEdge.withDirection(reverseDirection);
        return Lists.newArrayList(edge, otherDirectEdge);
    }
}
