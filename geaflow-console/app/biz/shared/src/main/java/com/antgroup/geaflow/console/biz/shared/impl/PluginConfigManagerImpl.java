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

package com.antgroup.geaflow.console.biz.shared.impl;

import com.antgroup.geaflow.console.biz.shared.PluginConfigManager;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.PluginConfigView;
import com.antgroup.geaflow.console.common.dal.entity.PluginConfigEntity;
import com.antgroup.geaflow.console.common.dal.model.PluginConfigSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.service.NameService;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PluginConfigManagerImpl extends
    NameManagerImpl<GeaflowPluginConfig, PluginConfigView, PluginConfigSearch> implements PluginConfigManager {

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Override
    protected NameViewConverter<GeaflowPluginConfig, PluginConfigView> getConverter() {
        return pluginConfigViewConverter;
    }

    @Override
    protected NameService<GeaflowPluginConfig, PluginConfigEntity, PluginConfigSearch> getService() {
        return pluginConfigService;
    }

    @Override
    protected List<GeaflowPluginConfig> parse(List<PluginConfigView> views) {
        return ListUtil.convert(views, v -> pluginConfigViewConverter.convert(v));
    }

    @Override
    public List<PluginConfigView> get(List<String> ids) {
        pluginConfigService.validateGetIds(ids);
        return super.get(ids);
    }

    @Override
    public boolean updateById(String id, PluginConfigView updateView) {
        pluginConfigService.validateUpdateIds(Collections.singletonList(id));
        return super.updateById(id, updateView);
    }

    public boolean drop(List<String> ids) {
        pluginConfigService.validateUpdateIds(ids);
        return super.drop(ids);
    }

    @Override
    public List<PluginConfigView> getPluginConfigs(GeaflowPluginCategory category, GeaflowPluginType type) {
        return build(pluginConfigService.getPluginConfigs(category, type));
    }
}
