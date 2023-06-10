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

import com.antgroup.geaflow.console.common.dal.entity.ViewEntity;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowView;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class ViewConverter extends DataConverter<GeaflowView, ViewEntity> {

    @Override
    protected ViewEntity modelToEntity(GeaflowView model) {
        ViewEntity entity = super.modelToEntity(model);
        entity.setCategory(model.getCategory());
        entity.setCode(model.getCode().getText());
        return entity;
    }

    @Override
    protected GeaflowView entityToModel(ViewEntity entity) {
        GeaflowView model = super.entityToModel(entity);
        model.setCategory(entity.getCategory());
        model.setCode(new GeaflowCode(entity.getCode()));
        return model;
    }

    public GeaflowView convert(ViewEntity entity, List<GeaflowField> fields) {
        GeaflowView view = entityToModel(entity);
        view.addFields(fields);
        return view;
    }
}
