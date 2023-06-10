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

import com.antgroup.geaflow.console.common.dal.entity.DataEntity;
import com.antgroup.geaflow.console.core.model.data.GeaflowData;

public abstract class DataConverter<M extends GeaflowData, E extends DataEntity> extends NameConverter<M, E> {

    @Override
    protected E modelToEntity(M model) {
        E entity = super.modelToEntity(model);
        String instanceId = model.getInstanceId();
        entity.setInstanceId(instanceId);
        return entity;
    }

    @Override
    protected M entityToModel(E entity) {
        M model = super.entityToModel(entity);
        model.setInstanceId(entity.getInstanceId());
        return model;
    }


    @Override
    protected M entityToModel(E entity, Class<? extends M> clazz) {
        M model = super.entityToModel(entity, clazz);
        model.setInstanceId(entity.getInstanceId());
        return model;
    }


}
