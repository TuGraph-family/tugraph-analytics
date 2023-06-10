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

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.binary.DecoderFactory;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import java.util.List;

public class DefaultRowDecoder implements RowDecoder {

    private final StructType rowType;
    private final IBinaryDecoder[] valueDecoders;

    public DefaultRowDecoder(StructType rowType) {
        this.rowType = rowType;
        this.valueDecoders = new IBinaryDecoder[rowType.size()];
        List<TableField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            valueDecoders[i] = generateDecoder(field.getType());
        }
    }

    private IBinaryDecoder generateDecoder(IType<?> type) {
        if (type instanceof VertexType) {
            return DecoderFactory.createVertexDecoder((VertexType) type);
        } else if (type instanceof EdgeType) {
            return DecoderFactory.createEdgeDecoder((EdgeType) type);
        } else if (type instanceof PathType) {
            return DecoderFactory.createPathDecoder((PathType) type);
        } else if (type instanceof StructType) {
            return DecoderFactory.createRowDecoder((StructType) type);
        }
        return null;
    }

    @Override
    public Row decode(Row row) {
        List<TableField> fields = rowType.getFields();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            if (valueDecoders[i] != null) {
                values[i] = valueDecoders[i].decode(row);
            } else {
                values[i] = decode(row.getField(i, field.getType()), field.getType());
            }
        }
        return ObjectRow.create(values);
    }

    private Object decode(Object o, IType<?> type) {
        if (type instanceof ArrayType) {
            if (o == null) {
                return null;
            }
            if (o.getClass().isArray()) {
                Object[] array = (Object[]) o;
                Object[] decodeArray = new Object[array.length];
                IType<?> componentType = ((ArrayType) type).getComponentType();
                IBinaryDecoder decoder = generateDecoder(componentType);
                for (int i = 0; i < array.length; i++) {
                    if (decoder != null) {
                        decodeArray[i] = decoder.decode((Row) array[i]);
                    } else {
                        decodeArray[i] = decode(array[i], componentType);
                    }
                }
                return decodeArray;
            }
        } else if (o instanceof BinaryString) {
            return o.toString();
        }
        return o;
    }
}
