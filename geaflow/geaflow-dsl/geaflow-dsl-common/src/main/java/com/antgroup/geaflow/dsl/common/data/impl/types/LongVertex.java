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

public class LongVertex implements RowVertex, KryoSerializable {

    public static final Supplier<LongVertex> CONSTRUCTOR = new Constructor();

    public long id;

    private BinaryString label;

    private Row value;

    public LongVertex() {

    }

    public LongVertex(long id) {
        this.id = id;
    }

    public LongVertex(long id, BinaryString label, Row value) {
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
        this.label = (BinaryString) BinaryUtil.toBinaryLabel(label);
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public void setId(Object id) {
        this.id = (long) Objects.requireNonNull(id);
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public LongVertex withValue(Row value) {
        return new LongVertex(id, label, value);
    }

    @Override
    public LongVertex withLabel(String label) {
        return new LongVertex(id, (BinaryString) BinaryUtil.toBinaryLabel(label), value);
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
        if (o instanceof LongVertex) {
            LongVertex that = (LongVertex) o;
            return id == that.id && Objects.equals(label, that.getBinaryLabel());
        } else {
            RowVertex that = (RowVertex) o;
            return that.equals(this);
        }
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

    private static class Constructor implements Supplier<LongVertex> {

        @Override
        public LongVertex get() {
            return new LongVertex();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize id, label and value
        output.writeLong(this.id);
        kryo.writeClassAndObject(output, this.getBinaryLabel());
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize id, label and value
        long id = input.readLong();
        BinaryString label = (BinaryString) kryo.readClassAndObject(input);
        Row value = (Row) kryo.readClassAndObject(input);
        this.id = id;
        this.setValue(value);
        this.setBinaryLabel(label);
    }

}
