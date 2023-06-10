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

package com.antgroup.geaflow.store.memory.csr.edge;

import com.antgroup.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.memory.csr.edge.type.IDEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.IDLabelEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.IDLabelTimeEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.IDTimeEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.ValueEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.ValueLabelEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.ValueLabelTimeEdgeArray;
import com.antgroup.geaflow.store.memory.csr.edge.type.ValueTimeEdgeArray;

public class EdgeArrayFactory {

    public static IEdgeArray getEdgeArray(GraphDataSchema dataSchema) {
        boolean noProperty = dataSchema.isEmptyEdgeProperty();
        GraphElementFlag flag = GraphElementFlag.build(dataSchema.getEdgeMeta().getGraphElementClass());
        IEdgeArray edgeArray;
        if (flag.isLabeledAndTimed()) {
            edgeArray = noProperty ? new IDLabelTimeEdgeArray<>() : new ValueLabelTimeEdgeArray<>();
        } else if (flag.isLabeled()) {
            edgeArray = noProperty ? new IDLabelEdgeArray<>() : new ValueLabelEdgeArray<>();
        } else if (flag.isTimed()) {
            edgeArray = noProperty ? new IDTimeEdgeArray<>() : new ValueTimeEdgeArray<>();
        } else {
            edgeArray = noProperty ? new IDEdgeArray<>() : new ValueEdgeArray<>();
        }
        return edgeArray;
    }
}
