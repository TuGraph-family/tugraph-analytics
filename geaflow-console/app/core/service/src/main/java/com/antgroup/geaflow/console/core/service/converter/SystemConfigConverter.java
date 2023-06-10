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

import com.antgroup.geaflow.console.common.dal.entity.SystemConfigEntity;
import com.antgroup.geaflow.console.core.model.config.GeaflowSystemConfig;
import org.springframework.stereotype.Component;

@Component
public class SystemConfigConverter extends NameConverter<GeaflowSystemConfig, SystemConfigEntity> {

    @Override
    protected SystemConfigEntity modelToEntity(GeaflowSystemConfig model) {
        SystemConfigEntity entity = super.modelToEntity(model);
        entity.setTenantId(model.getTenantId());
        entity.setValue(model.getValue());
        return entity;
    }

    @Override
    protected GeaflowSystemConfig entityToModel(SystemConfigEntity entity) {
        GeaflowSystemConfig model = super.entityToModel(entity);
        model.setValue(entity.getValue());
        return model;
    }

    public GeaflowSystemConfig convert(SystemConfigEntity entity) {
        return entityToModel(entity);
    }
}
