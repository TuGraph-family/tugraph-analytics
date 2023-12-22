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
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.PersistentArgsClass;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.runtime.TaskParams;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;
import java.util.Date;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PersistentDataStore implements GeaflowDataStore {

    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private PluginService pluginService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private InstanceService instanceService;

    @Override
    public Long queryStorageUsage(GeaflowTask task) {
        return null;
    }

    @Override
    public Long queryFileCount(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task.getDataPluginConfig(), task.getRelease().getVersion());
        FsPath path = getTaskPath(task);
        return persistentIO.getFileCount(path);
    }

    @Override
    public Date queryModifyTime(GeaflowTask task) {
        return null;
    }

    @Override
    public void cleanTaskData(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task.getDataPluginConfig(), task.getRelease().getVersion());
        FsPath path = getTaskPath(task);
        persistentIO.delete(path, true);
    }

    @Override
    public void cleanGraphData(GeaflowGraph graph) {
        // use default config
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowPluginConfig dataConfig = pluginConfigService.getDefaultPluginConfig(category, dataType);

        GeaflowVersion version = versionService.getDefaultVersion();
        IPersistentIO persistentIO = buildPersistentIO(dataConfig, version);

        PersistentArgsClass persistentArgs = new PersistentArgsClass(dataConfig);
        String root = persistentArgs.getRoot();
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);

        GeaflowInstance instance = instanceService.get(graph.getInstanceId());
        String pathSuffix = instance.getName() + "_" + graph.getName();

        FsPath path = classLoader.newInstance(FsPath.class, root, pathSuffix);
        persistentIO.delete(path, true);
        log.info("clean graph data {},{}", root, pathSuffix);
    }

    protected IPersistentIO buildPersistentIO(GeaflowPluginConfig pluginConfig, GeaflowVersion version) {
        PersistentArgsClass persistentArgs = new PersistentArgsClass(pluginConfig);
        Map<String, String> config = persistentArgs.build().toStringMap();

        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
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
