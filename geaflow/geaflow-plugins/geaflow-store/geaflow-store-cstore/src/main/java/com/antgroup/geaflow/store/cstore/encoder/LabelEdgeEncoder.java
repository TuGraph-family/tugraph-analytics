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

import com.antgroup.geaflow.model.graph.IGraphElementWithLabelField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.cstore.EdgeContainer;

public class LabelEdgeEncoder extends EdgeEncoder {

    public LabelEdgeEncoder(GraphDataSchema dataSchema) {
        super(dataSchema);
    }

    @Override
    public EdgeContainer encode(IEdge edge) {
        return new EdgeContainer(
            keyType.serialize(edge.getSrcId()),
            0,
            ((IGraphElementWithLabelField) edge).getLabel(),
            edge.getDirect() == EdgeDirection.OUT,
            keyType.serialize(edge.getTargetId()),
            emptyProperty ? null : valueSerializer.apply(edge.getValue()));
    }

    @Override
    public IEdge decode(EdgeContainer container) {
        IEdge edge = dataSchema.getEdgeConsFun().get();
        edge.setSrcId(this.keyType.deserialize(container.sid));
        edge.setTargetId(this.keyType.deserialize(container.tid));
        edge.setDirect(container.isOut ? EdgeDirection.OUT : EdgeDirection.IN);
        ((IGraphElementWithLabelField) edge).setLabel(container.label);
        return emptyProperty || container.property.length == 0 ? edge : edge.withValue(
            valueDeserializer.apply(container.property));
    }
}
