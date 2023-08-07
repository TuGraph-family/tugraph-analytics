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
import com.antgroup.geaflow.dsl.common.data.RowKey;
import com.antgroup.geaflow.dsl.common.data.RowKeyWithRequestId;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Objects;

public class DefaultRowKeyWithRequestId implements RowKeyWithRequestId {

    private final Object requestId;

    private final RowKey rowKey;

    public DefaultRowKeyWithRequestId(Object requestId, RowKey rowKey) {
        this.requestId = requestId;
        this.rowKey = rowKey;
    }

    @Override
    public Object getRequestId() {
        return requestId;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return rowKey.getField(i, type);
    }

    @Override
    public Object[] getKeys() {
        return rowKey.getKeys();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultRowKeyWithRequestId)) {
            return false;
        }
        DefaultRowKeyWithRequestId that = (DefaultRowKeyWithRequestId) o;
        return Objects.equals(requestId, that.requestId) && Objects.equals(rowKey, that.rowKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, rowKey);
    }

    public static class DefaultRowKeyWithRequestIdSerializer extends Serializer<DefaultRowKeyWithRequestId> {

        @Override
        public void write(Kryo kryo, Output output, DefaultRowKeyWithRequestId object) {
            kryo.writeClassAndObject(output, object.getRequestId());
            kryo.writeClassAndObject(output, object.rowKey);
        }

        @Override
        public DefaultRowKeyWithRequestId read(Kryo kryo, Input input, Class<DefaultRowKeyWithRequestId> aClass) {
            Object requestId = kryo.readClassAndObject(input);
            RowKey rowKey = (RowKey) kryo.readClassAndObject(input);
            return new DefaultRowKeyWithRequestId(requestId, rowKey);
        }

        @Override
        public DefaultRowKeyWithRequestId copy(Kryo kryo, DefaultRowKeyWithRequestId original) {
            return new DefaultRowKeyWithRequestId(original.requestId, original.rowKey);
        }
    }

}
