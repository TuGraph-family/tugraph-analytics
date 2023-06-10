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

package com.antgroup.geaflow.console.core.service.store.impl;

import com.antgroup.geaflow.console.common.service.integration.engine.Configuration;
import com.antgroup.geaflow.console.common.service.integration.engine.FsPath;
import com.antgroup.geaflow.console.common.service.integration.engine.IPersistentIO;
import com.antgroup.geaflow.console.common.service.integration.engine.PersistentIOBuilder;
import com.antgroup.geaflow.console.core.model.job.config.PersistentArgsClass;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.service.runtime.TaskParams;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;
import java.util.Date;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PersistentDataStore implements GeaflowDataStore {

    @Autowired
    private VersionFactory versionFactory;

    @Override
    public Long queryStorageUsage(GeaflowTask task) {
        return null;
    }

    @Override
    public Long queryFileCount(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task);
        FsPath path = getTaskPath(task);
        return persistentIO.getFileCount(path);
    }

    @Override
    public Date queryModifyTime(GeaflowTask task) {
        return null;
    }

    @Override
    public void cleanData(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task);
        FsPath path = getTaskPath(task);
        persistentIO.delete(path, true);
    }

    protected IPersistentIO buildPersistentIO(GeaflowTask task) {
        PersistentArgsClass persistentArgs = new PersistentArgsClass(task.getDataPluginConfig());
        Map<String, String> config = persistentArgs.build().toStringMap();

        VersionClassLoader classLoader = versionFactory.getClassLoader(task.getRelease().getVersion());
        Configuration configuration = classLoader.newInstance(Configuration.class, config);
        PersistentIOBuilder builder = classLoader.newInstance(PersistentIOBuilder.class);
        return builder.build(configuration);
    }

    protected FsPath getTaskPath(GeaflowTask task) {
        PersistentArgsClass persistentArgs = new PersistentArgsClass(task.getDataPluginConfig());
        String root = persistentArgs.getRoot();
        String pathSuffix = TaskParams.getRuntimeTaskName(task.getId());
        VersionClassLoader classLoader = versionFactory.getClassLoader(task.getRelease().getVersion());
        return classLoader.newInstance(FsPath.class, root, pathSuffix);
    }
}
