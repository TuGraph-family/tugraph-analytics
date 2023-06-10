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

import com.antgroup.geaflow.console.common.dal.entity.VertexEntity;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class VertexConverter extends DataConverter<GeaflowVertex, VertexEntity> {

    public GeaflowVertex convert(VertexEntity entity, List<GeaflowField> fields) {
        GeaflowVertex vertex = entityToModel(entity);
        vertex.addFields(fields);
        return vertex;
    }
}
