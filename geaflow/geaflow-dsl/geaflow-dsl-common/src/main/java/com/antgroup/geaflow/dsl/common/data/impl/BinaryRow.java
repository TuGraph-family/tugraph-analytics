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

package com.antgroup.geaflow.dsl.common.data.impl;

import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.NULL_BIT_OFFSET;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.getBitSetBytes;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.getFieldOffset;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.getFieldsNum;
import static com.antgroup.geaflow.dsl.common.binary.BinaryLayoutHelper.isSet;

import com.antgroup.geaflow.common.binary.HeapBinaryObject;
import com.antgroup.geaflow.common.binary.IBinaryObject;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.binary.FieldReaderFactory;
import com.antgroup.geaflow.dsl.common.binary.FieldReaderFactory.PropertyFieldReader;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Objects;

public class BinaryRow implements Row, KryoSerializable {

    private IBinaryObject binaryObject;

    private BinaryRow() {

    }

    private BinaryRow(byte[] bytes) {
        this.binaryObject = HeapBinaryObject.of(bytes);
    }

    public static BinaryRow of(byte[] bytes) {
        return new BinaryRow(bytes);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        if (isNullValue(i)) {
            return null;
        }
        PropertyFieldReader<?> reader = FieldReaderFactory.getPropertyFieldReader(type);
        int fieldsNum = getFieldsNum(binaryObject);
        long offset = getFieldOffset(getBitSetBytes(fieldsNum), i);
        return reader.read(binaryObject, offset);
    }

    @Override
    public String toString() {
        return "BinaryRow{" + "binaryObject=" + binaryObject + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryRow binaryRow = (BinaryRow) o;
        return Objects.equals(binaryObject, binaryRow.binaryObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(binaryObject);
    }

    private boolean isNullValue(int index) {
        return isSet(binaryObject, NULL_BIT_OFFSET, index);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        byte[] bytes = this.binaryObject.toBytes();
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int length = input.readInt();
        byte[] bytes = input.readBytes(length);
        this.binaryObject = HeapBinaryObject.of(bytes);
    }

}
