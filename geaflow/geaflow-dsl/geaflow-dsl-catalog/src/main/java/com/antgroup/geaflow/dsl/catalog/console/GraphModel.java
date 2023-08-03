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

package com.antgroup.geaflow.dsl.catalog.console;

import java.util.List;

public class GraphModel extends AbstractDataModel {

    private PluginConfigModel pluginConfig;

    private List<VertexModel> vertices;

    private List<EdgeModel> edges;

    private List<Endpoint> endpoints;

    public PluginConfigModel getPluginConfig() {
        return pluginConfig;
    }

    public void setPluginConfig(PluginConfigModel pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    public List<VertexModel> getVertices() {
        return vertices;
    }

    public void setVertices(List<VertexModel> vertices) {
        this.vertices = vertices;
    }

    public List<EdgeModel> getEdges() {
        return edges;
    }

    public void setEdges(List<EdgeModel> edges) {
        this.edges = edges;
    }


    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    public static class Endpoint {

        private final String sourceName;
        private final String targetName;
        private final String edgeName;

        public Endpoint(String srcVertex, String targetVertex, String edgeName) {
            this.sourceName = srcVertex;
            this.targetName = targetVertex;
            this.edgeName = edgeName;
        }

        public String getSourceName() {
            return sourceName;
        }

        public String getTargetName() {
            return targetName;
        }

        public String getEdgeName() {
            return edgeName;
        }

    }

}
