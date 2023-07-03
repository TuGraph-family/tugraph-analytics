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

import com.antgroup.geaflow.console.common.dal.entity.VersionEntity;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class VersionConverter extends NameConverter<GeaflowVersion, VersionEntity> {

    @Override
    protected VersionEntity modelToEntity(GeaflowVersion model) {
        VersionEntity version = super.modelToEntity(model);
        version.setPublish(model.isPublish());
        Optional.ofNullable(model.getEngineJarPackage()).map(GeaflowId::getId).ifPresent(version::setEngineJarId);
        Optional.ofNullable(model.getLangJarPackage()).map(GeaflowId::getId).ifPresent(version::setLangJarId);
        return version;
    }


    @Override
    protected GeaflowVersion entityToModel(VersionEntity entity) {
        GeaflowVersion geaflowVersion = super.entityToModel(entity);
        geaflowVersion.setPublish(entity.isPublish());
        return geaflowVersion;
    }

    public GeaflowVersion convert(VersionEntity entity, GeaflowRemoteFile engineJar, GeaflowRemoteFile langJar) {
        GeaflowVersion version = entityToModel(entity);
        version.setEngineJarPackage(engineJar);
        version.setLangJarPackage(langJar);
        return version;
    }
}
