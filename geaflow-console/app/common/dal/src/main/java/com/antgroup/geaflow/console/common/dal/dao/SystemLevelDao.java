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

package com.antgroup.geaflow.console.common.dal.dao;

import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.dal.mapper.GeaflowBaseMapper;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;

public abstract class SystemLevelDao<M extends GeaflowBaseMapper<E>, E extends IdEntity> extends GeaflowBaseDao<M, E> {

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (!ignoreTenant) {
            String message = "Mapper {} must be annotated by @InterceptorIgnore(tenantLine = \"true\")";
            throw new GeaflowException(message, mapperClass.getSimpleName());
        }
    }

}
