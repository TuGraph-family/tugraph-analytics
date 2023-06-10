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

package com.antgroup.geaflow.console.core.service.release;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.VersionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResolveVersionStage extends GeaflowBuildStage {

    @Autowired
    private VersionService versionService;

    public void init(GeaflowRelease release) {
        // get latest stable version
        GeaflowVersion version = versionService.getDefaultVersion();

        release.setVersion(version);
    }

    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        GeaflowVersion oldVersion = release.getVersion();
        GeaflowVersion newVersion = update.getNewVersion();

        // the version is deleted if oldVersion is null;
        if (oldVersion == null && newVersion == null) {
            throw new GeaflowException("Version is null");
        }

        if (newVersion == null) {
            return false;
        }

        if (oldVersion == null) {
            release.setVersion(newVersion);
            return true;
        }

        release.setVersion(newVersion);
        // compile in next stage if version is changed
        return !GeaflowVersion.md5Equals(oldVersion, newVersion);

    }
}
