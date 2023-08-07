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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IdOnlyVertex implements RowVertex, KryoSerializable {

    private Object id;

    private IdOnlyVertex() {

    }

    private IdOnlyVertex(Object id) {
        this.id = id;
    }

    public static IdOnlyVertex of(Object id) {
        return new IdOnlyVertex(id);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        if (i == VertexType.ID_FIELD_POSITION) {
            return id;
        }
        throw new IllegalArgumentException("Index out of range: " + i);
    }

    @Override
    public void setValue(Row value) {
        throw new IllegalArgumentException("Illegal call on setValue");
    }

    @Override
    public String getLabel() {
        throw new IllegalArgumentException("Illegal call on getLabel");
    }

    @Override
    public void setLabel(String label) {
        throw new IllegalArgumentException("Illegal call on setLabel");
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public void setId(Object id) {
        this.id = id;
    }

    @Override
    public Row getValue() {
        return Row.EMPTY;
    }

    @Override
    public IVertex<Object, Row> withValue(Row value) {
        throw new IllegalArgumentException("Illegal call on withValue");
    }

    @Override
    public IVertex<Object, Row> withLabel(String label) {
        throw new IllegalArgumentException("Illegal call on withLabel");
    }

    @Override
    public IVertex<Object, Row> withTime(long time) {
        throw new IllegalArgumentException("Illegal call on withTime");
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }

    @Override
    public BinaryString getBinaryLabel() {
        throw new IllegalArgumentException("Illegal call on getBinaryLabel");
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        throw new IllegalArgumentException("Illegal call on setBinaryLabel");
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, this.id);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.setId(kryo.readClassAndObject(input));
    }
}
