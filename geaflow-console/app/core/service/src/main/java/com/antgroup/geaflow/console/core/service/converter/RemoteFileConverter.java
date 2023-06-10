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

import com.antgroup.geaflow.console.common.dal.entity.RemoteFileEntity;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.file.GeaflowJarPackage;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileConverter extends NameConverter<GeaflowRemoteFile, RemoteFileEntity> {

    @Override
    protected RemoteFileEntity modelToEntity(GeaflowRemoteFile model) {
        RemoteFileEntity entity = super.modelToEntity(model);
        entity.setMd5(model.getMd5());
        entity.setType(model.getType());
        entity.setPath(model.getPath());
        entity.setUrl(model.getUrl());
        switch (model.getType()) {
            case JAR:
                entity.setEntryClass(((GeaflowJarPackage) model).getEntryClass());
                break;
            default:
                throw new GeaflowException("Unsupported file type {}", model.getType());

        }
        return entity;
    }

    @Override
    protected GeaflowRemoteFile entityToModel(RemoteFileEntity entity, Class<? extends GeaflowRemoteFile> clazz) {
        GeaflowRemoteFile model = super.entityToModel(entity, clazz);
        model.setPath(entity.getPath());
        model.setMd5(entity.getMd5());
        model.setUrl(entity.getUrl());
        return model;
    }

    public GeaflowRemoteFile convert(RemoteFileEntity entity) {
        GeaflowRemoteFile model = null;
        switch (entity.getType()) {
            case JAR:
                GeaflowJarPackage tmp = (GeaflowJarPackage) entityToModel(entity, GeaflowJarPackage.class);
                tmp.setEntryClass(entity.getEntryClass());
                model = tmp;
                break;
            default:
                throw new GeaflowException("Unsupported file type {}", entity.getType());
        }

        return model;
    }
}
