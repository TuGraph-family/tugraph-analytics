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

import com.antgroup.geaflow.console.biz.shared.view.NameView;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import java.util.Optional;

public abstract class NameViewConverter<M extends GeaflowName, V extends NameView> extends IdViewConverter<M, V> {

    @Override
    public void merge(V view, V updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getName()).ifPresent(view::setName);
        Optional.ofNullable(updateView.getComment()).ifPresent(view::setComment);
    }

    @Override
    protected V modelToView(M model) {
        V view = super.modelToView(model);
        view.setName(model.getName());
        view.setComment(model.getComment());
        return view;
    }

    @Override
    protected M viewToModel(V view) {
        M model = super.viewToModel(view);
        model.setName(view.getName());
        model.setComment(view.getComment());
        return model;
    }

    @Override
    protected M viewToModel(V view, Class<? extends M> clazz) {
        M model = super.viewToModel(view, clazz);
        model.setName(view.getName());
        model.setComment(view.getComment());
        return model;
    }
}
