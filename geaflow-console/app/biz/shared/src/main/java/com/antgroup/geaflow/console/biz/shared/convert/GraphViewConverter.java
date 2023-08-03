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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.EdgeView;
import com.antgroup.geaflow.console.biz.shared.view.EndpointView;
import com.antgroup.geaflow.console.biz.shared.view.GraphView;
import com.antgroup.geaflow.console.biz.shared.view.VertexView;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GraphViewConverter extends DataViewConverter<GeaflowGraph, GraphView> {

    @Autowired
    private VertexViewConverter vertexViewConverter;

    @Autowired
    private EdgeViewConverter edgeViewConverter;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Override
    public void merge(GraphView view, GraphView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getVertices()).ifPresent(view::setVertices);
        Optional.ofNullable(updateView.getEdges()).ifPresent(view::setEdges);
        Optional.ofNullable(updateView.getPluginConfig()).ifPresent(e -> {
            // update pluginConfig info
            e.setId(view.getPluginConfig().getId());
            e.setCategory(GeaflowPluginCategory.GRAPH);
            view.setPluginConfig(e);
        });
    }

    @Override
    protected GraphView modelToView(GeaflowGraph model) {
        GraphView graphView = super.modelToView(model);
        graphView.setPluginConfig(pluginConfigViewConverter.modelToView(model.getPluginConfig()));
        // cache model for vertex/edge
        HashMap<String, GeaflowVertex> vertexMap = new HashMap<>();
        HashMap<String, GeaflowEdge> edgeMap = new HashMap<>();
        List<VertexView> vertexViews = ListUtil.convert(model.getVertices().values(), e -> {
            vertexMap.putIfAbsent(e.getId(), e);
            return vertexViewConverter.modelToView(e);
        });
        List<EdgeView> edgeViews = ListUtil.convert(model.getEdges().values(), e -> {
            edgeMap.putIfAbsent(e.getId(), e);
            return edgeViewConverter.modelToView(e);
        });

        graphView.setVertices(vertexViews);
        graphView.setEdges(edgeViews);

        // set endpoints
        List<EndpointView> endpointViews = ListUtil.convert(model.getEndpoints(), e ->
            new EndpointView(edgeMap.get(e.getEdgeId()).getName(),
                vertexMap.get(e.getSourceId()).getName(),
                vertexMap.get(e.getTargetId()).getName())
        );
        graphView.setEndpoints(endpointViews);
        return graphView;
    }

    public GeaflowGraph convert(GraphView view, List<GeaflowVertex> vertices, List<GeaflowEdge> edges,
                                GeaflowPluginConfig pluginConfig) {
        GeaflowGraph graph = super.viewToModel(view);
        graph.addVertices(vertices);
        graph.addEdges(edges);
        graph.setPluginConfig(pluginConfig);
        return graph;
    }
}
