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

import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.NULL_BIT_OFFSET;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.getBitSetBytes;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.getExtendPoint;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.zeroBytes;

import com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper;
import com.antgroup.geaflow.dsl.common.binary.FieldWriterFactory;
import com.antgroup.geaflow.dsl.common.binary.FieldWriterFactory.PropertyFieldWriter;
import com.antgroup.geaflow.dsl.common.binary.HeapWriterBuffer;
import com.antgroup.geaflow.dsl.common.binary.WriterBuffer;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.BinaryRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import java.util.List;

public class DefaultRowEncoder implements RowEncoder {

    private final WriterBuffer writerBuffer;
    private final StructType rowType;

    public DefaultRowEncoder(StructType rowType) {
        this.writerBuffer = new HeapWriterBuffer();
        this.rowType = rowType;
        writerBuffer.initialize(BinaryLayoutHelper.getInitBufferSize(rowType.size()));
    }

    @Override
    public BinaryRow encode(Row row) {
        if (row instanceof BinaryRow) {
            return (BinaryRow) row;
        }
        writerBuffer.reset();
        // write fields num
        writerBuffer.writeInt(rowType.size());
        writerBuffer.setExtendPoint(getExtendPoint(rowType.size()));
        // write null bit set
        byte[] nullBitSet = new byte[getBitSetBytes(rowType.size())];
        if (nullBitSet.length > 0) {
            zeroBytes(nullBitSet);
            writerBuffer.writeBytes(nullBitSet);
        }
        // write all values
        List<TableField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            Object value = row.getField(i, field.getType());
            Object castValue = TypeCastUtil.cast(value, field.getType());
            PropertyFieldWriter writer = FieldWriterFactory
                .getPropertyFieldWriter(field.getType());
            try {
                writer.write(writerBuffer, NULL_BIT_OFFSET, i, castValue);
            } catch (Exception e) {
                throw new GeaFlowDSLException("Fail to write: " + field + ", value is: " + value,
                    e);
            }
        }
        byte[] rowBytes = (byte[]) writerBuffer.copyBuffer();
        return BinaryRow.of(rowBytes);
    }
}
