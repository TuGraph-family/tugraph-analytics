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

import com.antgroup.geaflow.console.biz.shared.view.UserView;
import com.antgroup.geaflow.console.core.model.security.GeaflowUser;
import org.springframework.stereotype.Component;

@Component
public class UserViewConverter extends NameViewConverter<GeaflowUser, UserView> {

    @Override
    protected UserView modelToView(GeaflowUser model) {
        UserView view = super.modelToView(model);
        view.setPhone(model.getPhone());
        view.setEmail(model.getEmail());
        return view;
    }

    @Override
    protected GeaflowUser viewToModel(UserView view) {
        GeaflowUser model = super.viewToModel(view);
        model.setPassword(view.getPassword());
        model.setPhone(view.getPhone());
        model.setEmail(view.getEmail());
        return model;
    }

    public GeaflowUser convert(UserView view) {
        return viewToModel(view);
    }
}
