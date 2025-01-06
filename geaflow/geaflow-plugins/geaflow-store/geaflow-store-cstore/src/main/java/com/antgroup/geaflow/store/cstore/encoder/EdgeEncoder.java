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

import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.cstore.EdgeContainer;
import com.antgroup.geaflow.store.encoder.BaseEncoder;
import java.util.function.Function;

public class EdgeEncoder extends BaseEncoder {

    public EdgeEncoder(GraphDataSchema dataSchema) {
        super(dataSchema);
    }

    @Override
    protected Function<Object, byte[]> initValueSerializer(GraphDataSchema dataSchema) {
        return dataSchema.getEdgePropertySerFun();
    }

    @Override
    protected Function<byte[], Object> initValueDeserializer(GraphDataSchema dataSchema) {
        return dataSchema.getEdgePropertyDeFun();
    }

    @Override
    protected boolean initEmptyProperty(GraphDataSchema dataSchema) {
        return dataSchema.isEmptyEdgeProperty();
    }

    public EdgeContainer encode(IEdge edge) {
        return new EdgeContainer(keyType.serialize(edge.getSrcId()), 0, "", edge.getDirect() == EdgeDirection.OUT,
            keyType.serialize(edge.getTargetId()),
            isEmptyProperty() ? null : getValueSerializer().apply(edge.getValue()));
    }

    public IEdge decode(EdgeContainer container) {
        IEdge edge = getDataSchema().getEdgeConsFun().get();
        edge.setSrcId(this.keyType.deserialize(container.sid));
        edge.setTargetId(this.keyType.deserialize(container.tid));
        edge.setDirect(container.isOut ? EdgeDirection.OUT : EdgeDirection.IN);
        return isEmptyProperty() || container.property.length == 0 ? edge : edge.withValue(
            getValueDeserializer().apply(container.property));
    }
}
