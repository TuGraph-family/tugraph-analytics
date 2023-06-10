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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.IdDao;
import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.dal.model.IdSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.service.converter.IdConverter;
import java.util.Collections;
import java.util.List;

public abstract class IdService<M extends GeaflowId, E extends IdEntity, S extends IdSearch> {

    protected abstract IdDao<E, S> getDao();

    protected abstract IdConverter<M, E> getConverter();

    private E build(M model) {
        return getConverter().convert(model);
    }

    private List<E> build(List<M> models) {
        return ListUtil.convert(models, this::build);
    }

    protected final M parse(E entity) {
        List<M> models = parse(Collections.singletonList(entity));
        return models.isEmpty() ? null : models.get(0);
    }

    protected abstract List<M> parse(List<E> entities);

    public boolean exist(String id) {
        return getDao().exist(id);
    }

    public PageList<M> search(S search) {
        PageList<E> pageList = getDao().search(search);
        return pageList.transform(this::parse);
    }

    public M get(String id) {
        if (id == null) {
            return null;
        }
        List<M> list = get(Collections.singletonList(id));
        return list.isEmpty() ? null : list.get(0);
    }

    public String create(M model) {
        if (model == null) {
            return null;
        }
        return create(Collections.singletonList(model)).get(0);
    }

    public boolean update(M model) {
        if (model == null) {
            return false;
        }
        return update(Collections.singletonList(model));
    }

    public boolean drop(String id) {
        if (id == null) {
            return false;
        }
        return drop(Collections.singletonList(id));
    }

    public List<M> get(List<String> ids) {
        List<E> entityList = getDao().get(ids);
        return parse(entityList);
    }

    public List<String> create(List<M> models) {
        List<E> entities = build(models);
        List<String> ids = getDao().create(entities);

        // fill id back to model
        for (int i = 0; i < models.size(); i++) {
            models.get(i).setId(ids.get(i));
        }
        return ids;
    }

    public boolean update(List<M> models) {
        return getDao().update(build(models));
    }

    public boolean drop(List<String> ids) {
        return getDao().drop(ids);
    }
}
