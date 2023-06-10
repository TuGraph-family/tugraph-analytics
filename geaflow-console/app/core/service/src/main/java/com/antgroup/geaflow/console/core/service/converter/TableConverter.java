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

import com.antgroup.geaflow.console.common.dal.entity.TableEntity;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowTable;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class TableConverter extends DataConverter<GeaflowTable, TableEntity> {

    @Override
    protected TableEntity modelToEntity(GeaflowTable model) {
        TableEntity entity = super.modelToEntity(model);
        entity.setPluginConfigId(model.getPluginConfig().getId());
        return entity;
    }

    public GeaflowTable convert(TableEntity entity, List<GeaflowField> fields, GeaflowPluginConfig pluginConfig) {
        GeaflowTable table = entityToModel(entity);
        table.addFields(fields);
        table.setPluginConfig(pluginConfig);
        return table;
    }
}
