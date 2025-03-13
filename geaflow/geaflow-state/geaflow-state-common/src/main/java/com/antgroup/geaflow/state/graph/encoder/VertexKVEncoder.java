/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.state.graph.encoder;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import java.util.ArrayList;
import java.util.List;

public class VertexKVEncoder<K, VV> extends VertexKVEncoderWithoutValue<K, VV> {

    public VertexKVEncoder(GraphDataSchema graphDataSchema, IBytesEncoder bytesEncoder) {
        super(graphDataSchema, bytesEncoder);
    }

    @Override
    public Tuple<byte[], byte[]> format(IVertex<K, VV> vertex) {
        byte[] a = keyType.serialize(vertex.getId());

        List<byte[]> bytes = new ArrayList<>();
        for (int i = 1; i < vertexSchema.size(); i++) {
            bytes.add(vertexSchema.get(i).getBinaryValue(vertex, graphDataSchema));
        }
        bytes.add(graphDataSchema.getVertexPropertySerFun().apply(vertex.getValue()));
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
        return vertex.withValue(graphDataSchema.getVertexPropertyDeFun().apply(values.get(i - 1)));
    }
}
