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

import com.antgroup.geaflow.console.common.dal.entity.PluginEntity;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class PluginConverter extends NameConverter<GeaflowPlugin, PluginEntity> {

    @Override
    protected PluginEntity modelToEntity(GeaflowPlugin model) {
        PluginEntity entity = super.modelToEntity(model);
        entity.setPluginType(model.getType());
        entity.setPluginCategory(model.getCategory());
        entity.setVersion(model.getVersion());
        entity.setEntryClass(model.getEntryClass());
        entity.setJarPackageId(Optional.ofNullable(model.getJarPackage()).map(GeaflowId::getId).orElse(null));
        entity.setDataPluginId(Optional.ofNullable(model.getDataPlugin()).map(GeaflowId::getId).orElse(null));
        return entity;
    }

    @Override
    protected GeaflowPlugin entityToModel(PluginEntity entity) {
        GeaflowPlugin model = super.entityToModel(entity);
        model.setVersion(entity.getVersion());
        model.setType(entity.getPluginType());
        model.setCategory(entity.getPluginCategory());
        model.setEntryClass(entity.getEntryClass());
        return model;
    }

    public GeaflowPlugin convert(PluginEntity entity, GeaflowPlugin dataPlugin, GeaflowRemoteFile jarPackage) {
        GeaflowPlugin model = entityToModel(entity);
        model.setDataPlugin(dataPlugin);
        model.setJarPackage(jarPackage);
        return model;
    }
}
