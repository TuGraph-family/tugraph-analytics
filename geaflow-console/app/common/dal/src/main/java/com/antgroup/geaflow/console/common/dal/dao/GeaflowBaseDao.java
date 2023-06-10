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
import com.antgroup.geaflow.console.common.dal.wrapper.GeaflowLambdaQueryChainWrapper;
import com.antgroup.geaflow.console.common.dal.wrapper.GeaflowLambdaUpdateChainWrapper;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.plugins.InterceptorIgnoreHelper;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.InitializingBean;

public abstract class GeaflowBaseDao<M extends GeaflowBaseMapper<E>, E extends IdEntity> extends
    ServiceImpl<M, E> implements InitializingBean {

    protected boolean ignoreTenant;

    protected String tableName;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.ignoreTenant = InterceptorIgnoreHelper.willIgnoreTenantLine(mapperClass.getName() + ".*");
        this.tableName = entityClass.getAnnotation(TableName.class).value();
    }

    @Override
    public LambdaQueryChainWrapper<E> lambdaQuery() {
        QueryWrapper<E> wrapper = new QueryWrapper<>();
        configQueryWrapper(wrapper);
        return new GeaflowLambdaQueryChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    @Override
    public LambdaUpdateChainWrapper<E> lambdaUpdate() {
        UpdateWrapper<E> wrapper = new UpdateWrapper<>();
        configUpdateWrapper(wrapper);
        return new GeaflowLambdaUpdateChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    public void configQueryWrapper(QueryWrapper<E> wrapper) {

    }

    public void configUpdateWrapper(UpdateWrapper<E> wrapper) {

    }
}
