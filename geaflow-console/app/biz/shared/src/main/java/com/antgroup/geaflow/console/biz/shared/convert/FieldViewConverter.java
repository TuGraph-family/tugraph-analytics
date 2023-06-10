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

import com.antgroup.geaflow.console.biz.shared.view.FieldView;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import org.springframework.stereotype.Component;

@Component
public class FieldViewConverter extends NameViewConverter<GeaflowField, FieldView> {

    @Override
    protected FieldView modelToView(GeaflowField model) {
        FieldView fieldView = super.modelToView(model);
        fieldView.setType(model.getType());
        fieldView.setCategory(model.getCategory());
        return fieldView;
    }

    @Override
    protected GeaflowField viewToModel(FieldView view) {
        GeaflowField field = super.viewToModel(view);
        field.setType(view.getType());
        field.setCategory(view.getCategory());
        return field;
    }

    public GeaflowField convert(FieldView view) {
        return viewToModel(view);
    }
}
