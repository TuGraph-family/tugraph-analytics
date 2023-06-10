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

import com.antgroup.geaflow.console.common.dal.entity.AuthorizationEntity;
import com.antgroup.geaflow.console.common.dal.mapper.AuthorizationMapper;
import com.antgroup.geaflow.console.common.dal.model.AuthorizationSearch;
import com.antgroup.geaflow.console.common.util.type.GeaflowAuthorityType;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class AuthorizationDao extends TenantLevelDao<AuthorizationMapper, AuthorizationEntity> implements
    IdDao<AuthorizationEntity, AuthorizationSearch> {

    public boolean exist(String userId, GeaflowAuthorityType authorityType, GeaflowResourceType resourceType,
                         String resourceId) {
        return lambdaQuery().eq(AuthorizationEntity::getUserId, userId)
            .eq(AuthorizationEntity::getResourceId, resourceId)
            .eq(AuthorizationEntity::getResourceType, resourceType)
            .and(e -> e.eq(AuthorizationEntity::getAuthorityType, authorityType).or()
                // ALL includes other types
                .eq(authorityType != GeaflowAuthorityType.ALL, AuthorizationEntity::getAuthorityType, GeaflowAuthorityType.ALL))
            .exists();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<AuthorizationEntity> wrapper, AuthorizationSearch search) {
        wrapper.eq(search.getAuthorityType() != null, AuthorizationEntity::getAuthorityType, search.getAuthorityType());
        wrapper.eq(search.getResourceId() != null, AuthorizationEntity::getResourceId, search.getResourceId());
        wrapper.eq(search.getResourceType() != null, AuthorizationEntity::getResourceType, search.getResourceType());
        wrapper.eq(search.getUserId() != null, AuthorizationEntity::getUserId, search.getUserId());
    }

    public boolean dropByResources(List<String> resourceIds, GeaflowResourceType type) {
        return lambdaUpdate().in(AuthorizationEntity::getResourceId, resourceIds)
            .eq(AuthorizationEntity::getResourceType, type)
            .remove();
    }
}
