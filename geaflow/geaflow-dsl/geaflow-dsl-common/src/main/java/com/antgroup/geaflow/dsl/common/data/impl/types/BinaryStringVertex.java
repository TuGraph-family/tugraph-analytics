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

package com.antgroup.geaflow.dsl.common.data.impl.types;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Objects;
import java.util.function.Supplier;

public class BinaryStringVertex implements RowVertex, KryoSerializable {

    public static final Supplier<BinaryStringVertex> CONSTRUCTOR = new Constructor();

    private BinaryString id;

    private BinaryString label;

    private Row value;

    public BinaryStringVertex() {

    }

    public BinaryStringVertex(BinaryString id) {
        this.id = id;
    }

    public BinaryStringVertex(BinaryString id, BinaryString label, Row value) {
        this.id = id;
        this.label = label;
        this.value = value;
    }

    @Override
    public String getLabel() {
        return label.toString();
    }

    @Override
    public void setLabel(String label) {
        this.label = BinaryString.fromString(label);
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public void setId(Object id) {
        this.id = (BinaryString) Objects.requireNonNull(id);
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public BinaryStringVertex withValue(Row value) {
        return new BinaryStringVertex(id, label, value);
    }

    @Override
    public BinaryStringVertex withLabel(String label) {
        return new BinaryStringVertex(id, (BinaryString) BinaryUtil.toBinaryLabel(label), value);
    }

    @Override
    public IVertex<Object, Row> withTime(long time) {
        throw new GeaFlowDSLException("Vertex not support timestamp");
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Object o) {
        RowVertex vertex = (RowVertex) o;
        return ((Comparable) id).compareTo(vertex.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowVertex)) {
            return false;
        }
        RowVertex that = (RowVertex) o;
        return id.equals(that.getId()) && Objects.equals(label, that.getBinaryLabel());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        switch (i) {
            case VertexType.ID_FIELD_POSITION:
                return id;
            case VertexType.LABEL_FIELD_POSITION:
                return label;
            default:
                return value.getField(i - 2, type);
        }
    }

    @Override
    public void setValue(Row value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return id + "#" + label + "#" + value;
    }

    @Override
    public BinaryString getBinaryLabel() {
        return label;
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        this.label = label;
    }

    private static class Constructor implements Supplier<BinaryStringVertex> {

        @Override
        public BinaryStringVertex get() {
            return new BinaryStringVertex();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize fields
        byte[] idBytes = this.id.getBytes();
        byte[] labelBytes = this.getBinaryLabel().getBytes();
        output.writeInt(idBytes.length);
        output.writeBytes(idBytes);
        output.writeInt(labelBytes.length);
        output.writeBytes(labelBytes);
        // serialize value
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize fields
        BinaryString id = BinaryString.fromBytes(input.readBytes(input.readInt()));
        BinaryString label = BinaryString.fromBytes(input.readBytes(input.readInt()));
        // deserialize value
        Row value = (Row) kryo.readClassAndObject(input);
        // create vertex object
        this.id = id;
        this.setValue(value);
        this.setBinaryLabel(label);
    }

}
