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

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.cstore.VertexContainer;

public class TimeVertexEncoder extends VertexEncoder {

    public TimeVertexEncoder(GraphDataSchema dataSchema) {
        super(dataSchema);
    }

    @Override
    public VertexContainer encode(IVertex vertex) {
        return new VertexContainer(
            keyType.serialize(vertex.getId()),
            ((IGraphElementWithTimeField) vertex).getTime(),
            "",
            emptyProperty ? null : valueSerializer.apply(vertex.getValue()));
    }

    @Override
    public IVertex decode(VertexContainer vertexContainer) {
        IVertex vertex = dataSchema.getVertexConsFun().get();
        vertex.setId(this.keyType.deserialize(vertexContainer.id));
        ((IGraphElementWithTimeField) vertex).setTime(vertexContainer.ts);
        return emptyProperty || vertexContainer.property.length == 0 ? vertex : vertex.withValue(
            valueDeserializer.apply(vertexContainer.property));
    }
}
