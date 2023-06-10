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
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;

public class GeaflowLambdaQueryChainWrapper<E extends IdEntity> extends LambdaQueryChainWrapper<E> {

    public GeaflowLambdaQueryChainWrapper(BaseMapper<E> baseMapper, LambdaQueryWrapper<E> queryWrapper) {
        super(baseMapper);
        super.wrapperChildren = queryWrapper;
    }
}
