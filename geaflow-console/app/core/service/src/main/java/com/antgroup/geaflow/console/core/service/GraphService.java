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
import com.antgroup.geaflow.console.common.dal.dao.EndpointDao;
import com.antgroup.geaflow.console.common.dal.dao.GraphDao;
import com.antgroup.geaflow.console.common.dal.dao.GraphStructMappingDao;
import com.antgroup.geaflow.console.common.dal.entity.EndpointEntity;
import com.antgroup.geaflow.console.common.dal.entity.GraphEntity;
import com.antgroup.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.dal.model.GraphSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowEndpoint;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.service.converter.DataConverter;
import com.antgroup.geaflow.console.core.service.converter.GraphConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
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

    @Autowired
    private EndpointDao endpointDao;

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
            List<GeaflowEndpoint> endpoints = ListUtil.convert(endpointDao.getByGraphId(g.getId()),
                e -> new GeaflowEndpoint(e.getEdgeId(), e.getSourceId(), e.getTargetId()));
            return graphConverter.convert(g, vertices, edges, endpoints, pluginConfig);
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
            updateGraphEndpoints(newGraph, vertexIds, edgeIds);
            GeaflowPluginConfig pluginConfig = newGraph.getPluginConfig();
            pluginConfigService.update(pluginConfig);
        }

        return super.update(models);
    }

    private void updateGraphEndpoints(GeaflowGraph graph, List<String> vertexIds, List<String> edgeIds) {
        List<EndpointEntity> entities = endpointDao.getByGraphId(graph.getId());
        // drop endpoints whose vertex/edge not in new graph.
        List<String> dropIds = entities.stream().filter(e -> !edgeIds.contains(e.getEdgeId())
            || !vertexIds.contains(e.getSourceId())
            || !vertexIds.contains(e.getTargetId())).map(IdEntity::getId).collect(Collectors.toList());
        endpointDao.drop(dropIds);
    }

    @Override
    public boolean drop(List<String> ids) {
        List<GraphEntity> entities = graphDao.get(ids);
        // do not delete edges and vertices

        graphStructMappingDao.removeByGraphIds(ids);
        endpointDao.dropByGraphIds(ids);
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

    public boolean createEndpoints(GeaflowGraph graph, List<GeaflowEndpoint> endpoints) {
        // validate vertex/edge of endpoints exist in graph
        validateEndpoints(graph, endpoints);
        // do not insert if exist
        List<EndpointEntity> entities = endpoints.stream()
            .filter(e -> !endpointDao.exists(graph.getId(), e.getEdgeId(), e.getSourceId(), e.getTargetId()))
            .map(e -> new EndpointEntity(graph.getId(), e.getEdgeId(), e.getSourceId(), e.getTargetId()))
            .distinct()
            .collect(Collectors.toList());

        endpointDao.create(entities);
        return true;
    }

    private void validateEndpoints(GeaflowGraph graph, List<GeaflowEndpoint> geaflowEndpoints) {
        Set<String> edgeIds = graph.getEdges().values().stream().map(GeaflowId::getId).collect(Collectors.toSet());
        Set<String> vertexIds = graph.getVertices().values().stream().map(GeaflowId::getId).collect(Collectors.toSet());
        for (GeaflowEndpoint endpoint : geaflowEndpoints) {
            if (!edgeIds.contains(endpoint.getEdgeId())) {
                throw new GeaflowException("Edge {} not exits in graph", edgeService.getNameById(endpoint.getEdgeId()));
            }

            if (!vertexIds.contains(endpoint.getSourceId())) {
                throw new GeaflowException("Vertex {} not exits in graph",vertexService.getNameById(endpoint.getSourceId()));
            }

            if (!vertexIds.contains(endpoint.getTargetId())) {
                throw new GeaflowException("Vertex {} not exits in graph", vertexService.getNameById(endpoint.getTargetId()));
            }
        }
    }

    public boolean deleteEndpoints(GeaflowGraph graph, List<GeaflowEndpoint> endpoints) {
        // delete all endpoints if input is empty
        if (CollectionUtils.isEmpty(endpoints)) {
            return endpointDao.dropByGraphIds(Collections.singletonList(graph.getId()));
        }
        for (GeaflowEndpoint e : endpoints) {
            endpointDao.dropByEndpoint(graph.getId(), e.getEdgeId(), e.getSourceId(), e.getTargetId());
        }
        return true;
    }

    public void checkBindingRelations(String resourceId, GeaflowResourceType resourceType) {
        // check if vertex/edge used in graph
        List<GraphStructMappingEntity> entities = graphStructMappingDao.getByResourceId(resourceId, resourceType);
        if (CollectionUtils.isNotEmpty(entities)) {
            throw new GeaflowException("{} {} is used in Graph {}", resourceType, resourceId,
                this.getNameById(entities.get(0).getGraphId()));
        }
    }
}
