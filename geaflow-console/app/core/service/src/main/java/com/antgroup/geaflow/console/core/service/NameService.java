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

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.entity.NameEntity;
import com.antgroup.geaflow.console.common.dal.model.NameSearch;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class NameService<M extends GeaflowName,
    E extends NameEntity, S extends NameSearch> extends IdService<M, E, S> {

    @Override
    protected abstract NameDao<E, S> getDao();

    @Override
    protected abstract NameConverter<M, E> getConverter();

    public boolean existName(String name) {
        return getDao().existName(name);
    }

    public M getByName(String name) {
        if (name == null) {
            return null;
        }
        List<M> users = getByNames(Collections.singletonList(name));
        return users.isEmpty() ? null : users.get(0);
    }

    public boolean dropByName(String name) {
        if (name == null) {
            return false;
        }
        return dropByNames(Collections.singletonList(name));
    }

    public String getIdByName(String name) {
        if (name == null) {
            return null;
        }
        return getIdsByNames(Collections.singletonList(name)).get(name);
    }


    public List<M> getByNames(List<String> names) {
        List<E> entityList = getDao().getByNames(names);
        return parse(entityList);
    }

    public boolean dropByNames(List<String> names) {
        Map<String, String> idsByNames = getIdsByNames(names);
        return this.drop(new ArrayList<>(idsByNames.values()));
    }

    public Map<String, String> getIdsByNames(List<String> names) {
        return getDao().getIdsByNames(names);
    }

    public String getNameById(String id) {
        E e = getDao().get(id);
        return e != null ? e.getName() : null;
    }

}
