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

import com.antgroup.geaflow.console.common.dal.entity.UserRoleMappingEntity;
import com.antgroup.geaflow.console.common.dal.mapper.UserRoleMappingMapper;
import com.antgroup.geaflow.console.common.dal.model.IdSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowRoleType;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class UserRoleMappingDao extends TenantLevelExtDao<UserRoleMappingMapper, UserRoleMappingEntity> implements
    IdDao<UserRoleMappingEntity, IdSearch> {

    public List<GeaflowRoleType> getRoleTypes(String tenantId, String userId) {
        List<UserRoleMappingEntity> entities = lambdaQuery(tenantId).select(UserRoleMappingEntity::getRoleType)
            .eq(UserRoleMappingEntity::getUserId, userId).list();
        return ListUtil.convert(entities, UserRoleMappingEntity::getRoleType);
    }

    public boolean existRoleType(String tenantId, String userId, GeaflowRoleType roleType) {
        return lambdaQuery(tenantId).eq(UserRoleMappingEntity::getUserId, userId)
            .eq(UserRoleMappingEntity::getRoleType, roleType).exists();
    }

    public void addRoleType(String tenantId, String userId, GeaflowRoleType roleType) {
        UserRoleMappingEntity entity = new UserRoleMappingEntity();
        entity.setTenantId(tenantId);
        entity.setUserId(userId);
        entity.setRoleType(roleType);
        create(entity);
    }

    public void deleteRoleType(String tenantId, String userId, GeaflowRoleType roleType) {
        lambdaUpdate(tenantId).eq(UserRoleMappingEntity::getUserId, userId)
            .eq(UserRoleMappingEntity::getRoleType, roleType).remove();
    }
}
