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

import com.antgroup.geaflow.console.biz.shared.view.PluginView;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PluginViewConverter extends NameViewConverter<GeaflowPlugin, PluginView> {

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Override
    public void merge(PluginView view, PluginView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getJarPackage()).ifPresent(view::setJarPackage);
        Optional.ofNullable(updateView.getType()).ifPresent(view::setType);
        Optional.ofNullable(updateView.getCategory()).ifPresent(view::setCategory);
    }

    @Override
    protected PluginView modelToView(GeaflowPlugin model) {
        PluginView view = super.modelToView(model);
        view.setType(model.getType());
        view.setCategory(model.getCategory());
        view.setJarPackage(Optional.ofNullable(model.getJarPackage()).map(e -> remoteFileViewConverter.convert(e)).orElse(null));
        view.setSystem(model.isSystem());
        return view;
    }

    @Override
    protected GeaflowPlugin viewToModel(PluginView view) {
        GeaflowPlugin model = super.viewToModel(view);
        model.setType(view.getType());
        model.setCategory(view.getCategory());
        return model;
    }

    public GeaflowPlugin convert(PluginView view, GeaflowRemoteFile jarPackage) {
        GeaflowPlugin model = viewToModel(view);
        model.setJarPackage(jarPackage);
        return model;
    }
}
