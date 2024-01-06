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
import com.antgroup.geaflow.console.common.dal.mapper.EndpointMapper;
import com.antgroup.geaflow.console.common.dal.model.IdSearch;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class EndpointDao extends TenantLevelDao<EndpointMapper, EndpointEntity> implements
    IdDao<EndpointEntity, IdSearch> {

    public List<EndpointEntity> getByGraphId(String graphId) {
        return lambdaQuery().eq(EndpointEntity::getGraphId, graphId).list();
    }

    public EndpointEntity getByEndpoint(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaQuery()
            .eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).one();
    }

    public boolean exists(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaQuery().eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).exists();
    }


    public boolean dropByGraphIds(List<String> graphIds) {
        return lambdaUpdate().eq(EndpointEntity::getGraphId, graphIds).remove();
    }

    public boolean dropByEndpoint(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaUpdate()
            .eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).remove();
    }

}
