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

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.dal.entity.LLMEntity;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import org.springframework.stereotype.Component;

@Component
public class LLMConverter extends NameConverter<GeaflowLLM, LLMEntity> {
    
    @Override
    protected LLMEntity modelToEntity(GeaflowLLM model) {
        LLMEntity entity = super.modelToEntity(model);
        entity.setType(model.getType());
        entity.setUrl(model.getUrl());
        entity.setArgs(JSON.toJSONString(model.getArgs()));
        return entity;
    }

    @Override
    protected GeaflowLLM entityToModel(LLMEntity entity) {
        GeaflowLLM model = super.entityToModel(entity);
        model.setType(entity.getType());
        model.setUrl(entity.getUrl());
        model.setArgs(JSON.parseObject(entity.getArgs(), GeaflowConfig.class));
        return model;
    }

    public GeaflowLLM convert(LLMEntity entity) {
        return entityToModel(entity);
    }
}
