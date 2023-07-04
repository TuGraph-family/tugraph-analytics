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
import com.antgroup.geaflow.console.common.dal.dao.PluginConfigDao;
import com.antgroup.geaflow.console.common.dal.entity.PluginConfigEntity;
import com.antgroup.geaflow.console.common.dal.model.PluginConfigSearch;
import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.I18nUtil;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.ConfigDescFactory;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigDesc;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.PluginConfigClass;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.PluginConfigConverter;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PluginConfigService extends NameService<GeaflowPluginConfig, PluginConfigEntity, PluginConfigSearch> {

    @Autowired
    private PluginConfigDao pluginConfigDao;

    @Autowired
    private PluginConfigConverter pluginConfigConverter;

    @Autowired
    private PluginService pluginService;

    @Override
    protected NameDao<PluginConfigEntity, PluginConfigSearch> getDao() {
        return pluginConfigDao;
    }

    @Override
    protected NameConverter<GeaflowPluginConfig, PluginConfigEntity> getConverter() {
        return pluginConfigConverter;
    }

    @Override
    protected List<GeaflowPluginConfig> parse(List<PluginConfigEntity> entities) {
        return ListUtil.convert(entities, e -> pluginConfigConverter.convert(e));
    }

    public List<GeaflowPluginConfig> getPluginConfigs(GeaflowPluginCategory category, GeaflowPluginType type) {
        return parse(pluginConfigDao.getPluginConfigs(category, type));
    }

    public GeaflowPluginConfig getDefaultPluginConfig(GeaflowPluginCategory category, GeaflowPluginType type) {
        List<GeaflowPluginConfig> pluginConfigs = getPluginConfigs(category, type);
        if (pluginConfigs.isEmpty()) {
            throw new GeaflowException("At least one plugin config for {} plugin type {} needed", category, type);
        }

        return pluginConfigs.get(0);
    }

    public void validateGetIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginConfigDao.validateGetId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)",
                    id);
            }
        }
    }

    public void validateUpdateIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginConfigDao.validateUpdateId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)",
                    id);
            }
        }
    }

    public GeaflowPluginConfig getDefaultPluginConfig(GeaflowPluginCategory category) {
        List<GeaflowPlugin> plugins = pluginService.getPlugins(category);
        if (plugins.isEmpty()) {
            return null;
        }

        GeaflowPluginType type = plugins.get(0).getType();
        List<GeaflowPluginConfig> pluginConfigs = getPluginConfigs(category, type);
        if (pluginConfigs.isEmpty()) {
            return null;
        }

        return pluginConfigs.get(0);
    }

    @Transactional
    public String createDefaultPluginConfig(GeaflowPluginConfig pluginConfig) {
        GeaflowPluginType type = pluginConfig.getType();
        GeaflowPluginCategory category = pluginConfig.getCategory();

        // check plugin config
        List<GeaflowPluginConfig> pluginConfigs = getPluginConfigs(category, type);
        if (!pluginConfigs.isEmpty()) {
            throw new GeaflowIllegalException("Default {} plugin {} config exists", category, type);
        }

        // check plugin
        List<GeaflowPlugin> plugins = pluginService.getPlugins(category);
        if (!plugins.isEmpty()) {
            throw new GeaflowIllegalException("Default {} plugin exists", category);
        }

        // create plugin config
        final String pluginConfigId = create(pluginConfig);

        // create plugin
        GeaflowPlugin plugin = new GeaflowPlugin();
        plugin.setName(Fmt.as("plugin-{}-{}-default", category, type).toLowerCase());
        plugin.setComment(Fmt.as(I18nUtil.getMessage("i18n.key.default.plugin.comment.format"), category, type).toLowerCase());
        plugin.setType(type);
        plugin.setCategory(category);
        pluginService.create(plugin);

        return pluginConfigId;
    }

    public void testConnection(GeaflowPluginConfig pluginConfig) {
        GeaflowPluginType type = pluginConfig.getType();
        GeaflowConfig config = pluginConfig.getConfig();

        GeaflowConfigDesc configDesc = ConfigDescFactory.get(type);
        GeaflowConfigClass configClass = config.parse(configDesc.getClazz());
        ((PluginConfigClass) configClass).testConnection();
    }
}
