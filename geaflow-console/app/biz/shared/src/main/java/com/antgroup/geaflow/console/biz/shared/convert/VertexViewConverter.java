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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.VertexView;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class VertexViewConverter extends StructViewConverter<GeaflowVertex, VertexView> {

    @Override
    protected VertexView modelToView(GeaflowVertex model) {
        VertexView vertexView = super.modelToView(model);
        vertexView.setType(GeaflowStructType.VERTEX);
        return vertexView;
    }

    @Override
    public void merge(VertexView view, VertexView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getFields()).ifPresent(view::setFields);
    }

    @Override
    protected GeaflowVertex viewToModel(VertexView view) {
        GeaflowVertex vertex = super.viewToModel(view);
        vertex.setType(GeaflowStructType.VERTEX);
        return vertex;
    }

    public GeaflowVertex converter(VertexView view, List<GeaflowField> fields) {
        GeaflowVertex vertex = viewToModel(view);
        vertex.addFields(fields);
        return vertex;
    }
}
