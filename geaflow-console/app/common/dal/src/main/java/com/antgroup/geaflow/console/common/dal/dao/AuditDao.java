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

import com.antgroup.geaflow.console.common.dal.entity.AuditEntity;
import com.antgroup.geaflow.console.common.dal.mapper.AuditMapper;
import com.antgroup.geaflow.console.common.dal.model.AuditSearch;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

@Repository
public class AuditDao extends TenantLevelDao<AuditMapper, AuditEntity> implements IdDao<AuditEntity, AuditSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<AuditEntity> wrapper, AuditSearch search) {
        wrapper.eq(search.getResourceType() != null, AuditEntity::getResourceType, search.getResourceType())
            .eq(StringUtils.isNotBlank(search.getResourceId()), AuditEntity::getResourceId, search.getResourceId())
            .eq(search.getOperationType() != null, AuditEntity::getOperationType, search.getOperationType());
    }

}
