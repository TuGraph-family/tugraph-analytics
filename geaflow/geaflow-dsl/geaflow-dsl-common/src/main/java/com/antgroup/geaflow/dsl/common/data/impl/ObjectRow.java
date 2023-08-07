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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;

public class ObjectRow implements Row {

    private final Object[] fields;

    private ObjectRow(Object[] fields) {
        this.fields = fields;
    }

    public static ObjectRow create(Object... fields) {
        return new ObjectRow(fields);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return fields[i];
    }

    @Override
    public String toString() {
        return Arrays.toString(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectRow)) {
            return false;
        }
        ObjectRow objectRow = (ObjectRow) o;
        return Arrays.equals(fields, objectRow.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    public static class ObjectRowSerializer extends Serializer<ObjectRow> {

        @Override
        public void write(Kryo kryo, Output output, ObjectRow objectRow) {
            output.writeInt(objectRow.fields.length);
            for (Object field : objectRow.fields) {
                kryo.writeClassAndObject(output, field);
            }
        }

        @Override
        public ObjectRow read(Kryo kryo, Input input, Class<ObjectRow> aClass) {
            int size = input.readInt();
            Object[] fields = new Object[size];
            for (int i = 0; i < size; i++) {
                fields[i] = kryo.readClassAndObject(input);
            }
            return ObjectRow.create(fields);
        }

        @Override
        public ObjectRow copy(Kryo kryo, ObjectRow original) {
            return ObjectRow.create(original.fields);
        }
    }
}
