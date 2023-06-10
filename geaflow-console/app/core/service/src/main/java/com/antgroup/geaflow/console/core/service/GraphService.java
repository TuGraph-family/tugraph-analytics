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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.DataDao;
import com.antgroup.geaflow.console.common.dal.dao.GraphDao;
import com.antgroup.geaflow.console.common.dal.dao.GraphStructMappingDao;
import com.antgroup.geaflow.console.common.dal.entity.GraphEntity;
import com.antgroup.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import com.antgroup.geaflow.console.common.dal.model.GraphSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.service.converter.DataConverter;
import com.antgroup.geaflow.console.core.service.converter.GraphConverter;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GraphService extends DataService<GeaflowGraph, GraphEntity, GraphSearch> {

    @Autowired
    private GraphDao graphDao;

    @Autowired
    private VertexService vertexService;

    @Autowired
    private EdgeService edgeService;

    @Autowired
    private GraphStructMappingDao graphStructMappingDao;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private GraphConverter graphConverter;

    @Override
    protected DataDao<GraphEntity, GraphSearch> getDao() {
        return graphDao;
    }

    @Override
    protected DataConverter<GeaflowGraph, GraphEntity> getConverter() {
        return graphConverter;
    }

    @Override
    public List<String> create(List<GeaflowGraph> graphs) {
        List<String> ids = super.create(graphs);
        for (GeaflowGraph g : graphs) {
            List<String> vertices = ListUtil.convert(g.getVertices().values(), GeaflowId::getId);
            List<String> edges = ListUtil.convert(g.getEdges().values(), GeaflowId::getId);
            saveGraphStructs(g, vertices, edges);
        }

        return ids;
    }

    @Override
    protected List<GeaflowGraph> parse(List<GraphEntity> entities) {
        return ListUtil.convert(entities, g -> {
            // get Vertices and edges.
            String id = g.getId();
            List<GeaflowVertex> vertices = vertexService.getVerticesByGraphId(id);
            List<GeaflowEdge> edges = edgeService.getEdgesByGraphId(id);
            GeaflowPluginConfig pluginConfig = pluginConfigService.get(g.getPluginConfigId());

            return graphConverter.convert(g, vertices, edges, pluginConfig);
        });
    }

    @Override
    public boolean update(List<GeaflowGraph> models) {
        List<String> ids = ListUtil.convert(models, GeaflowId::getId);
        graphStructMappingDao.removeByGraphIds(ids);
        // update vertices and edges
        for (GeaflowGraph newGraph : models) {
            List<String> vertexIds = ListUtil.convert(newGraph.getVertices().values(), GeaflowId::getId);
            List<String> edgeIds = ListUtil.convert(newGraph.getEdges().values(), GeaflowId::getId);
            saveGraphStructs(newGraph, vertexIds, edgeIds);

            GeaflowPluginConfig pluginConfig = newGraph.getPluginConfig();
            pluginConfigService.update(pluginConfig);
        }

        return super.update(models);
    }

    @Override
    public boolean drop(List<String> ids) {
        List<GraphEntity> entities = graphDao.get(ids);
        // do not delete edges and vertices

        graphStructMappingDao.removeByGraphIds(ids);
        pluginConfigService.drop(ListUtil.convert(entities, GraphEntity::getPluginConfigId));
        return super.drop(ids);
    }

    private void saveGraphStructs(GeaflowGraph g, List<String> vertexIds, List<String> edgeIds) {
        List<GraphStructMappingEntity> graphStructs = new ArrayList<>();
        String graphId = g.getId();

        for (int i = 0; i < vertexIds.size(); i++) {
            String id = vertexIds.get(i);
            GraphStructMappingEntity entity = new GraphStructMappingEntity(graphId, id, GeaflowResourceType.VERTEX, i);
            graphStructs.add(entity);
        }

        for (int i = 0; i < edgeIds.size(); i++) {
            String id = edgeIds.get(i);
            GraphStructMappingEntity entity = new GraphStructMappingEntity(graphId, id, GeaflowResourceType.EDGE, i);
            graphStructs.add(entity);
        }

        if (!graphStructs.isEmpty()) {
            graphStructMappingDao.create(graphStructs);
        }
    }
}
