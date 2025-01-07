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
package com.antgroup.geaflow.store.cstore.encoder;

import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.cstore.VertexContainer;
import com.antgroup.geaflow.store.encoder.BaseEncoder;
import java.util.function.Function;

public class VertexEncoder extends BaseEncoder {

    public VertexEncoder(GraphDataSchema dataSchema) {
        super(dataSchema);
    }

    @Override
    protected Function<Object, byte[]> initValueSerializer(GraphDataSchema dataSchema) {
        return dataSchema.getVertexPropertySerFun();
    }

    @Override
    protected Function<byte[], Object> initValueDeserializer(GraphDataSchema dataSchema) {
        return dataSchema.getVertexPropertyDeFun();
    }

    @Override
    protected boolean initEmptyProperty(GraphDataSchema dataSchema) {
        return dataSchema.isEmptyVertexProperty();
    }

    public VertexContainer encode(IVertex vertex) {
        return new VertexContainer(keyType.serialize(vertex.getId()), 0, "",
            isEmptyProperty() ? null : getValueSerializer().apply(vertex.getValue()));
    }

    public IVertex decode(VertexContainer vertexContainer) {
        IVertex vertex = getDataSchema().getVertexConsFun().get();
        vertex.setId(this.keyType.deserialize(vertexContainer.id));
        return isEmptyProperty() || vertexContainer.property.length == 0 ? vertex : vertex.withValue(
            getValueDeserializer().apply(vertexContainer.property));
    }
}
