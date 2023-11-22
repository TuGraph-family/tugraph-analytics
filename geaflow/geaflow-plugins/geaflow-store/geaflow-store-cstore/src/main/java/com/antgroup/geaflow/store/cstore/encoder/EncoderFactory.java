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

import com.antgroup.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import com.antgroup.geaflow.state.schema.GraphDataSchema;

public class EncoderFactory {

    public static VertexEncoder getVertexEncoder(GraphDataSchema dataSchema) {
        GraphElementFlag flag = GraphElementFlag.build(
            dataSchema.getVertexMeta().getGraphElementClass());
        if (flag.isTimed()) {
            return new TimeVertexEncoder(dataSchema);
        } else if (flag.isLabeled()) {
            return new LabelVertexEncoder(dataSchema);
        } else if (flag.isLabeledAndTimed()) {
            return new LabelTimeVertexEncoder(dataSchema);
        } else {
            return new VertexEncoder(dataSchema);
        }
    }

    public static EdgeEncoder getEdgeEncoder(GraphDataSchema dataSchema) {
        GraphElementFlag flag = GraphElementFlag.build(
            dataSchema.getEdgeMeta().getGraphElementClass());
        if (flag.isTimed()) {
            return new TimeEdgeEncoder(dataSchema);
        } else if (flag.isLabeled()) {
            return new LabelEdgeEncoder(dataSchema);
        } else if (flag.isLabeledAndTimed()) {
            return new LabelTimeEdgeEncoder(dataSchema);
        } else {
            return new EdgeEncoder(dataSchema);
        }
    }
}
