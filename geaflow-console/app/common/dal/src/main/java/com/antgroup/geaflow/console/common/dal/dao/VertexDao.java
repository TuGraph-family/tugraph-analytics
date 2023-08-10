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

package com.antgroup.geaflow.console.common.dal.dao;

import com.antgroup.geaflow.console.common.dal.entity.EndpointEntity;
import com.antgroup.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import com.antgroup.geaflow.console.common.dal.entity.VertexEntity;
import com.antgroup.geaflow.console.common.dal.mapper.EndpointMapper;
import com.antgroup.geaflow.console.common.dal.mapper.GraphStructMappingMapper;
import com.antgroup.geaflow.console.common.dal.mapper.VertexMapper;
import com.antgroup.geaflow.console.common.dal.model.VertexSearch;
import com.github.yulichang.wrapper.MPJLambdaWrapper;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class VertexDao extends TenantLevelDao<VertexMapper, VertexEntity> implements DataDao<VertexEntity, VertexSearch> {

    @Autowired
    private GraphStructMappingMapper graphStructMappingMapper;

    @Autowired
    private EndpointMapper endpointMapper;


    public List<VertexEntity> getByGraphId(String graphId) {
        MPJLambdaWrapper<GraphStructMappingEntity> wrapper = new MPJLambdaWrapper<GraphStructMappingEntity>().selectAll(
                VertexEntity.class)
            .innerJoin(VertexEntity.class, VertexEntity::getId, GraphStructMappingEntity::getResourceId)
            .eq(GraphStructMappingEntity::getGraphId, graphId)
            .orderByAsc(GraphStructMappingEntity::getSortKey);
        return graphStructMappingMapper.selectJoinList(VertexEntity.class, wrapper);

    }

    public List<VertexEntity> getByEdge(String edgeId) {
        MPJLambdaWrapper<EndpointEntity> wrapper = new MPJLambdaWrapper<EndpointEntity>().selectAll(
                VertexEntity.class).innerJoin(VertexEntity.class, VertexEntity::getId,
                EndpointEntity::getSourceId)
            .eq(EndpointEntity::getEdgeId, edgeId);
        return endpointMapper.selectJoinList(VertexEntity.class, wrapper);
    }
}
