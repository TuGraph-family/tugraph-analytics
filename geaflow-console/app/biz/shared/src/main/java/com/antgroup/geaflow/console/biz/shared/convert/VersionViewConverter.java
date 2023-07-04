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

import com.antgroup.geaflow.console.biz.shared.view.VersionView;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class VersionViewConverter extends NameViewConverter<GeaflowVersion, VersionView> {

    @Autowired
    protected RemoteFileViewConverter remoteFileViewConverter;

    @Override
    public void merge(VersionView view, VersionView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getEngineJarPackage()).ifPresent(view::setEngineJarPackage);
        Optional.ofNullable(updateView.getLangJarPackage()).ifPresent(view::setLangJarPackage);
        Optional.ofNullable(updateView.getPublish()).ifPresent(view::setPublish);
    }

    @Override
    protected VersionView modelToView(GeaflowVersion model) {
        VersionView view = super.modelToView(model);
        view.setEngineJarPackage(remoteFileViewConverter.convert(model.getEngineJarPackage()));
        view.setLangJarPackage(remoteFileViewConverter.convert(model.getLangJarPackage()));
        view.setPublish(model.isPublish());
        return view;
    }

    @Override
    protected GeaflowVersion viewToModel(VersionView view) {
        GeaflowVersion model = super.viewToModel(view);
        Optional.ofNullable(view.getPublish()).ifPresent(model::setPublish);
        return model;
    }

    public GeaflowVersion convert(VersionView view, GeaflowRemoteFile engineJarPackage,
                                  GeaflowRemoteFile langJarPackage) {
        GeaflowVersion version = viewToModel(view);
        version.setEngineJarPackage(engineJarPackage);
        version.setLangJarPackage(langJarPackage);
        return version;
    }
}
