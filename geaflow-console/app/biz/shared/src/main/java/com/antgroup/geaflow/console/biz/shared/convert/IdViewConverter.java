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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.IdView;
import com.antgroup.geaflow.console.common.util.DateTimeUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@SuppressWarnings("unchecked")
public abstract class IdViewConverter<M extends GeaflowId, V extends IdView> {

    public void merge(V view, V updateView) {

    }

    public V convert(M model) {
        return modelToView(model);
    }

    protected V modelToView(M model) {
        Type[] args = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();

        try {
            V view = (V) ((Class<?>) args[1]).newInstance();

            view.setTenantId(model.getTenantId());
            view.setId(model.getId());
            view.setCreateTime(DateTimeUtil.format(model.getGmtCreate()));
            view.setCreatorId(model.getCreatorId());
            view.setModifyTime(DateTimeUtil.format(model.getGmtModified()));
            view.setModifierId(model.getModifierId());

            return view;

        } catch (Exception e) {
            throw new GeaflowException("Convert id model to view failed", e);
        }
    }

    protected M viewToModel(V view) {
        Type[] args = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();

        try {
            M model = (M) ((Class<?>) args[0]).newInstance();

            model.setId(view.getId());

            return model;

        } catch (Exception e) {
            throw new GeaflowException("Convert id view to model failed", e);
        }
    }

    protected M viewToModel(V view, Class<? extends M> clazz) {
        try {
            M model = clazz.newInstance();
            model.setId(view.getId());
            return model;
        } catch (Exception e) {
            throw new GeaflowException("Convert id view to model failed", e);
        }
    }
}
