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

import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.dal.entity.PluginEntity;
import com.antgroup.geaflow.console.common.dal.mapper.PluginMapper;
import com.antgroup.geaflow.console.common.dal.model.PluginSearch;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

@Repository
public class PluginDao extends UserLevelDao<PluginMapper, PluginEntity> implements NameDao<PluginEntity, PluginSearch> {

    public List<PluginEntity> getPlugins(GeaflowPluginCategory category) {
        return lambdaQuery().eq(PluginEntity::getPluginCategory, category).list();
    }

    public PluginEntity getPlugin(String type, GeaflowPluginCategory category) {
        return lambdaQuery()
            .eq(PluginEntity::getPluginType, type)
            .eq(PluginEntity::getPluginCategory, category).one();
    }

    public List<PluginEntity> getSystemPlugins(GeaflowPluginCategory category) {
        return lambdaQuery().eq(PluginEntity::getPluginCategory, category).eq(PluginEntity::isSystem, true).list();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<PluginEntity> wrapper, PluginSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        wrapper.eq(search.getPluginType() != null, PluginEntity::getPluginType, search.getPluginType())
            .eq(search.getPluginCategory() != null, PluginEntity::getPluginCategory, search.getPluginCategory())
            .eq(PluginEntity::isSystem, systemSession)
            .and(StringUtils.isNotEmpty(search.getKeyword()), e -> e.like(PluginEntity::getName, search.getKeyword())
                .or().like(PluginEntity::getPluginCategory, search.getKeyword())
                .or().like(PluginEntity::getPluginType, search.getKeyword()));
    }

    public long getFileRefCount(String fileId, String excludeId) {
        return lambdaQuery().eq(PluginEntity::getJarPackageId, fileId)
            .ne(excludeId != null, IdEntity::getId, excludeId)
            .count();
    }
}
