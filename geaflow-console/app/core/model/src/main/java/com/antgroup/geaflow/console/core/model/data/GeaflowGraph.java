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

package com.antgroup.geaflow.console.core.model.data;

import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowGraph extends GeaflowData {

    private final Map<String, GeaflowVertex> vertices = new LinkedHashMap<>();

    private final Map<String, GeaflowEdge> edges = new LinkedHashMap<>();

    private List<GeaflowEndpoint> endpoints = new ArrayList<>();

    private GeaflowPluginConfig pluginConfig;

    public GeaflowGraph(String name, String comment) {
        super.name = name;
        super.comment = comment;
    }

    public void addVertices(List<GeaflowVertex> vertices) {
        for (GeaflowVertex vertex : vertices) {
            this.vertices.put(vertex.getName(), vertex);
        }
    }

    public void addEdges(List<GeaflowEdge> edges) {
        for (GeaflowEdge edge : edges) {
            this.edges.put(edge.getName(), edge);
        }
    }

    public void addVertex(GeaflowVertex vertex) {
        vertices.put(vertex.getName(), vertex);
    }

    public void addEdge(GeaflowEdge edge) {
        edges.put(edge.getName(), edge);
    }


    public void removeVertex(String vertexName) {
        vertices.remove(vertexName);
    }

    public void removeEdge(String edgeName) {
        edges.remove(edgeName);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(pluginConfig, "pluginConfig is null");
    }
}
