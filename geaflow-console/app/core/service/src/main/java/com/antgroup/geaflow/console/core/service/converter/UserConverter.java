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

import com.antgroup.geaflow.console.common.dal.entity.UserEntity;
import com.antgroup.geaflow.console.common.util.Md5Util;
import com.antgroup.geaflow.console.core.model.security.GeaflowUser;
import org.springframework.stereotype.Component;

@Component
public class UserConverter extends NameConverter<GeaflowUser, UserEntity> {

    @Override
    protected UserEntity modelToEntity(GeaflowUser model) {
        UserEntity entity = super.modelToEntity(model);
        entity.setPhone(model.getPhone());
        entity.setEmail(model.getEmail());
        entity.setPasswordSign(Md5Util.encodeString(model.getPassword()));
        return entity;
    }

    @Override
    protected GeaflowUser entityToModel(UserEntity entity) {
        GeaflowUser model = super.entityToModel(entity);
        model.setPhone(entity.getPhone());
        model.setEmail(entity.getEmail());
        return model;
    }

    public GeaflowUser convert(UserEntity entity) {
        GeaflowUser user = entityToModel(entity);
        user.setPhone(entity.getPhone());
        user.setEmail(entity.getEmail());
        return user;
    }
}
