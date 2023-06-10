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

import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@SuppressWarnings("unchecked")
public abstract class IdConverter<M extends GeaflowId, E extends IdEntity> {

    public E convert(M model) {
        return modelToEntity(model);
    }

    protected E modelToEntity(M model) {
        Type[] args = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();

        try {
            E entity = (E) ((Class<?>) args[1]).newInstance();

            entity.setId(model.getId());
            entity.setCreatorId(model.getCreatorId());
            entity.setModifierId(model.getModifierId());
            entity.setGmtCreate(model.getGmtCreate());
            entity.setGmtModified(model.getGmtModified());
            return entity;

        } catch (Exception e) {
            throw new GeaflowException("Convert id model to entity failed", e);
        }
    }

    protected M entityToModel(E entity) {
        try {
            Type[] args = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();
            M model = (M) ((Class<?>) args[0]).newInstance();
            setProperty(model, entity);
            return model;
        } catch (Exception e) {
            throw new GeaflowException("Convert id entity to model failed", e);
        }
    }

    protected M entityToModel(E entity, Class<? extends M> clazz) {
        try {
            M model = clazz.newInstance();
            setProperty(model, entity);
            return model;
        } catch (Exception e) {
            throw new GeaflowException("Convert id entity to model failed", e);
        }
    }

    private void setProperty(M model, E entity) {
        model.setTenantId(entity.getTenantId());
        model.setId(entity.getId());
        model.setCreatorId(entity.getCreatorId());
        model.setModifierId(entity.getModifierId());
        model.setGmtCreate(entity.getGmtCreate());
        model.setGmtModified(entity.getGmtModified());
    }
}
