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

package com.antgroup.geaflow.state.descriptor;

import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.state.schema.GraphDataSchema;

public class GraphStateDescriptor<K, VV, EV> extends BaseStateDescriptor {

    private GraphDataSchema graphSchema;

    private GraphStateDescriptor(String name, String storeType) {
        super(name, storeType);
    }

    @Override
    public DescriptorType getDescriptorType() {
        return DescriptorType.GRAPH;
    }

    public static <K, VV, EV> GraphStateDescriptor<K, VV, EV> build(String name, String storeType) {
        return new GraphStateDescriptor<>(name, storeType);
    }

    public GraphStateDescriptor<K, VV, EV> withGraphMeta(GraphMeta descriptor) {
        this.graphSchema = new GraphDataSchema(descriptor);
        return this;
    }

    public GraphDataSchema getGraphSchema() {
        return graphSchema;
    }
}
