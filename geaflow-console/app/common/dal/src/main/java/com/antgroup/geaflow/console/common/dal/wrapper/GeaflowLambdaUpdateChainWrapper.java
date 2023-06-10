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

package com.antgroup.geaflow.console.common.dal.wrapper;

import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import java.util.Date;
import lombok.Getter;

@Getter
public class GeaflowLambdaUpdateChainWrapper<E extends IdEntity> extends LambdaUpdateChainWrapper<E> {

    public GeaflowLambdaUpdateChainWrapper(BaseMapper<E> baseMapper, LambdaUpdateWrapper<E> updateWrapper) {
        super(baseMapper);
        super.wrapperChildren = updateWrapper;
    }

    @Override
    public boolean update() {
        this.set(E::getGmtModified, new Date());
        this.set(E::getModifierId, ContextHolder.get().getUserId());
        return super.update();
    }
}
