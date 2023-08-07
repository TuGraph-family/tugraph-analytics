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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.dao.PluginDao;
import com.antgroup.geaflow.console.common.dal.entity.PluginEntity;
import com.antgroup.geaflow.console.common.dal.model.PluginSearch;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.PluginConverter;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PluginService extends NameService<GeaflowPlugin, PluginEntity, PluginSearch> {

    @Autowired
    private PluginDao pluginDao;

    @Autowired
    private PluginConverter pluginConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Override
    protected NameDao<PluginEntity, PluginSearch> getDao() {
        return pluginDao;
    }

    @Override
    protected NameConverter<GeaflowPlugin, PluginEntity> getConverter() {
        return pluginConverter;
    }

    @Override
    protected List<GeaflowPlugin> parse(List<PluginEntity> pluginEntities) {
        return pluginEntities.stream().map(e -> {
            GeaflowPlugin dataPlugin = this.get(e.getDataPluginId());
            GeaflowRemoteFile jarPackage = remoteFileService.get(e.getJarPackageId());
            return pluginConverter.convert(e, dataPlugin, jarPackage);
        }).collect(Collectors.toList());
    }

    public List<GeaflowPlugin> getPlugins(GeaflowPluginCategory category) {
        List<PluginEntity> plugins = pluginDao.getPlugins(category);
        return parse(plugins);
    }

    public GeaflowPlugin getDefaultPlugin(GeaflowPluginCategory category) {
        List<GeaflowPlugin> plugins = getPlugins(category);
        if (plugins.size() != 1) {
            throw new GeaflowException("At least one plugin for {} plugin needed", category);
        }

        return plugins.get(0);
    }

    public List<GeaflowPlugin> getSystemPlugins(GeaflowPluginCategory category) {
        List<PluginEntity> plugins = pluginDao.getSystemPlugins(category);
        return parse(plugins);
    }

    public GeaflowPlugin getDefaultSystemPlugin(GeaflowPluginCategory category) {
        List<GeaflowPlugin> plugins = getSystemPlugins(category);
        if (plugins.size() != 1) {
            throw new GeaflowException("At least one system plugin for {} plugin needed", category);
        }

        return plugins.get(0);
    }

    public void validateGetIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginDao.validateGetId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }

    public void validateUpdateIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginDao.validateUpdateId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }

}
