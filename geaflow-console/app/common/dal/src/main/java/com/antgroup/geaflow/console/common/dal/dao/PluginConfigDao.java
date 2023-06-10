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

import com.antgroup.geaflow.console.common.dal.entity.PluginConfigEntity;
import com.antgroup.geaflow.console.common.dal.mapper.PluginConfigMapper;
import com.antgroup.geaflow.console.common.dal.model.PluginConfigSearch;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class PluginConfigDao extends UserLevelDao<PluginConfigMapper, PluginConfigEntity> implements
    NameDao<PluginConfigEntity, PluginConfigSearch> {

    public List<PluginConfigEntity> getPluginConfigs(GeaflowPluginCategory category, GeaflowPluginType type) {
        return lambdaQuery().eq(PluginConfigEntity::getCategory, category).eq(PluginConfigEntity::getType, type).list();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<PluginConfigEntity> wrapper, PluginConfigSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        wrapper.eq(search.getType() != null, PluginConfigEntity::getType, search.getType())
            .eq(search.getCategory() != null, PluginConfigEntity::getCategory, search.getCategory())
            .eq(PluginConfigEntity::isSystem, systemSession);
    }
}
