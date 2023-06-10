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

import com.antgroup.geaflow.console.biz.shared.view.EdgeView;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class EdgeViewConverter extends StructViewConverter<GeaflowEdge, EdgeView> {

    @Override
    protected EdgeView modelToView(GeaflowEdge model) {
        EdgeView edgeView = super.modelToView(model);
        edgeView.setType(GeaflowStructType.EDGE);
        return edgeView;
    }

    @Override
    public void merge(EdgeView view, EdgeView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getFields()).ifPresent(view::setFields);
    }

    @Override
    protected GeaflowEdge viewToModel(EdgeView view) {
        GeaflowEdge edge = super.viewToModel(view);
        edge.setType(GeaflowStructType.EDGE);
        return edge;
    }

    public GeaflowEdge converter(EdgeView view, List<GeaflowField> fields) {
        GeaflowEdge edge = viewToModel(view);
        edge.addFields(fields);
        return edge;
    }
}
