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

package com.antgroup.geaflow.console.core.service.converter;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.dal.entity.PluginConfigEntity;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.stereotype.Component;

@Component
public class PluginConfigConverter extends NameConverter<GeaflowPluginConfig, PluginConfigEntity> {

    @Override
    protected PluginConfigEntity modelToEntity(GeaflowPluginConfig model) {
        PluginConfigEntity entity = super.modelToEntity(model);
        entity.setType(model.getType());
        entity.setConfig(JSON.toJSONString(model.getConfig()));
        entity.setCategory(model.getCategory());
        return entity;
    }

    @Override
    protected GeaflowPluginConfig entityToModel(PluginConfigEntity entity) {
        GeaflowPluginConfig model = super.entityToModel(entity);
        model.setType(entity.getType());
        model.setConfig(JSON.parseObject(entity.getConfig(), GeaflowConfig.class));
        model.setCategory(entity.getCategory());
        return model;
    }

    public GeaflowPluginConfig convert(PluginConfigEntity entity) {
        return entityToModel(entity);
    }
}
