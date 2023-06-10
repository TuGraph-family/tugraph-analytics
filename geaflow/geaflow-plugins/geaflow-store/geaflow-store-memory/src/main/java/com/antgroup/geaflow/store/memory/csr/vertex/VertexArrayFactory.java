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

package com.antgroup.geaflow.store.memory.csr.vertex;

import com.antgroup.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.memory.csr.vertex.type.IDLabelTimeVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.IDLabelVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.IDTimeVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.IDVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.ValueLabelTimeVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.ValueLabelVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.ValueTimeVertexArray;
import com.antgroup.geaflow.store.memory.csr.vertex.type.ValueVertexArray;

public class VertexArrayFactory {

    public static IVertexArray getVertexArray(GraphDataSchema dataSchema) {
        boolean noProperty = dataSchema.isEmptyVertexProperty();
        GraphElementFlag flag = GraphElementFlag.build(dataSchema.getVertexMeta().getGraphElementClass());
        IVertexArray vertexArray;
        if (flag.isLabeledAndTimed()) {
            vertexArray = noProperty ? new IDLabelTimeVertexArray<>() : new ValueLabelTimeVertexArray<>();
        } else if (flag.isLabeled()) {
            vertexArray = noProperty ? new IDLabelVertexArray<>() : new ValueLabelVertexArray<>();
        } else if (flag.isTimed()) {
            vertexArray = noProperty ? new IDTimeVertexArray<>() : new ValueTimeVertexArray<>();
        } else {
            vertexArray = noProperty ? new IDVertexArray<>() : new ValueVertexArray<>();
        }
        return vertexArray;
    }

}
