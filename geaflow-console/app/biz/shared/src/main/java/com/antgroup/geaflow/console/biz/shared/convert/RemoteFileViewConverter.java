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

import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileViewConverter extends NameViewConverter<GeaflowRemoteFile, RemoteFileView> {

    @Override
    protected RemoteFileView modelToView(GeaflowRemoteFile model) {
        RemoteFileView view = super.modelToView(model);
        view.setMd5(model.getMd5());
        view.setType(model.getType());
        view.setPath(model.getPath());
        view.setUrl(model.getUrl());
        return view;
    }


    @Override
    protected GeaflowRemoteFile viewToModel(RemoteFileView view) {
        GeaflowRemoteFile model = super.viewToModel(view);
        model.setPath(view.getPath());
        model.setMd5(view.getMd5());
        model.setUrl(view.getUrl());
        model.setType(view.getType());
        return model;
    }

    public GeaflowRemoteFile convert(RemoteFileView view) {
        return viewToModel(view);
    }
}
