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
import com.antgroup.geaflow.console.common.dal.entity.ClusterEntity;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import org.springframework.stereotype.Component;

@Component
public class ClusterConverter extends NameConverter<GeaflowCluster, ClusterEntity> {

    @Override
    protected ClusterEntity modelToEntity(GeaflowCluster model) {
        ClusterEntity entity = super.modelToEntity(model);
        entity.setType(model.getType());
        entity.setConfig(JSON.toJSONString(model.getConfig()));
        return entity;
    }

    @Override
    protected GeaflowCluster entityToModel(ClusterEntity entity) {
        GeaflowCluster model = super.entityToModel(entity);
        model.setType(entity.getType());
        model.setConfig(JSON.parseObject(entity.getConfig(), GeaflowConfig.class));
        return model;
    }

    public GeaflowCluster convert(ClusterEntity entity) {
        return entityToModel(entity);
    }
}
