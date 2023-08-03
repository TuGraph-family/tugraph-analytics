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

package com.antgroup.geaflow.dsl.common.descriptor;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class GraphDescriptor {

    private final AtomicLong id = new AtomicLong(0L);
    public List<NodeDescriptor> nodes = new ArrayList<>();
    public List<EdgeDescriptor> edges = new ArrayList<>();
    public List<RelationDescriptor> relations = new ArrayList<>();

    public GraphDescriptor addNode(NodeDescriptor nodeDescriptor) {
        nodes.add(Objects.requireNonNull(nodeDescriptor));
        return this;
    }

    public GraphDescriptor addNode(List<NodeDescriptor> nodeDescriptors) {
        for (NodeDescriptor nodeDescriptor : nodeDescriptors) {
            addNode(nodeDescriptor);
        }
        return this;
    }

    public GraphDescriptor addEdge(EdgeDescriptor edgeDescriptor) {
        edges.add(Objects.requireNonNull(edgeDescriptor));
        return this;
    }

    public GraphDescriptor addEdge(List<EdgeDescriptor> edgeStats) {
        for (EdgeDescriptor edgeDescriptor : edgeStats) {
            addEdge(edgeDescriptor);
        }
        return this;
    }

    public GraphDescriptor addRelation(RelationDescriptor relationDescriptor) {
        relations.add(Objects.requireNonNull(relationDescriptor));
        return this;
    }

    public String getIdName(String value) {
        return value + "-" + id.getAndIncrement();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
