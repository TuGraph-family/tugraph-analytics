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

import com.antgroup.geaflow.console.common.dal.entity.SystemConfigEntity;
import com.antgroup.geaflow.console.common.dal.mapper.SystemConfigMapper;
import com.antgroup.geaflow.console.common.dal.model.SystemConfigSearch;
import com.baomidou.mybatisplus.core.conditions.interfaces.Compare;
import com.baomidou.mybatisplus.core.conditions.interfaces.Func;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

@Repository
public class SystemConfigDao extends SystemLevelDao<SystemConfigMapper, SystemConfigEntity> implements
    NameDao<SystemConfigEntity, SystemConfigSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<SystemConfigEntity> wrapper, SystemConfigSearch search) {
        String tenantId = search.getTenantId();
        String value = search.getValue();
        wrapper.eq(StringUtils.isNotBlank(tenantId), SystemConfigEntity::getTenantId, tenantId);
        wrapper.like(StringUtils.isNotBlank(value), SystemConfigEntity::getValue, value);
    }

    public SystemConfigEntity get(String tenantId, String key) {
        return wrap(lambdaQuery(), tenantId).eq(SystemConfigEntity::getName, key).one();
    }

    public String getValue(String tenantId, String key) {
        SystemConfigEntity entity = wrap(lambdaQuery(), tenantId).select(SystemConfigEntity::getValue)
            .eq(SystemConfigEntity::getName, key).one();
        return Optional.ofNullable(entity).map(SystemConfigEntity::getValue).orElse(null);
    }

    public boolean setValue(String tenantId, String key, String value) {
        return wrap(lambdaUpdate(), tenantId).set(SystemConfigEntity::getValue, value)
            .eq(SystemConfigEntity::getName, key).update();
    }

    public boolean exist(String tenantId, String key) {
        return wrap(lambdaQuery(), tenantId).eq(SystemConfigEntity::getName, key).exists();
    }

    public boolean delete(String tenantId, String key) {
        return wrap(lambdaUpdate(), tenantId).eq(SystemConfigEntity::getName, key).remove();
    }

    private <W extends Compare<W, SFunction<SystemConfigEntity, ?>> & Func<W, SFunction<SystemConfigEntity, ?>>> W wrap(
        W wrapper, String tenantId) {
        if (StringUtils.isNotBlank(tenantId)) {
            wrapper.eq(SystemConfigEntity::getTenantId, tenantId);

        } else {
            wrapper.isNull(SystemConfigEntity::getTenantId);
        }
        return wrapper;
    }
}
