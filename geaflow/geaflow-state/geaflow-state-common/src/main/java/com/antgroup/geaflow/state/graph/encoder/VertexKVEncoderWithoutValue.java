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

package com.antgroup.geaflow.state.graph.encoder;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import java.util.ArrayList;
import java.util.List;

public class VertexKVEncoderWithoutValue<K, VV> implements IVertexKVEncoder<K, VV> {

    protected final GraphDataSchema graphDataSchema;
    protected final List<VertexAtom> vertexSchema;
    protected final IType keyType;
    protected final IBytesEncoder bytesEncoder;

    public VertexKVEncoderWithoutValue(GraphDataSchema graphDataSchema, IBytesEncoder bytesEncoder) {
        this.graphDataSchema = graphDataSchema;
        this.vertexSchema = graphDataSchema.getVertexAtoms();
        this.keyType = graphDataSchema.getKeyType();
        this.bytesEncoder = bytesEncoder;
    }

    @Override
    public Tuple<byte[], byte[]> format(IVertex<K, VV> vertex) {
        byte[] a = keyType.serialize(vertex.getId());

        List<byte[]> bytes = new ArrayList<>();
        for (int i = 1; i < vertexSchema.size(); i++) {
            bytes.add(vertexSchema.get(i).getBinaryValue(vertex, graphDataSchema));
        }

        return new Tuple<>(a, bytesEncoder.combine(bytes));
    }

    @Override
    public IVertex<K, VV> getVertex(byte[] key, byte[] value) {
        IVertex vertex = this.graphDataSchema.getVertexConsFun().get();
        vertex.setId(keyType.deserialize(key));
        List<byte[]> values = bytesEncoder.split(value);
        if (values == null) {
            IBytesEncoder encoder = BytesEncoderRepo.get(
                bytesEncoder.parseMagicNumber(key[value.length - 1]));
            values = encoder.split(value);
        }
        int i = 1;
        for (; i < vertexSchema.size(); i++) {
            vertexSchema.get(i).setBinaryValue(vertex, values.get(i - 1), graphDataSchema);
        }
        return vertex;
    }

    @Override
    public K getVertexID(byte[] key) {
        return (K) keyType.deserialize(key);
    }

    @Override
    public IBytesEncoder getBytesEncoder() {
        return this.bytesEncoder;
    }
}
