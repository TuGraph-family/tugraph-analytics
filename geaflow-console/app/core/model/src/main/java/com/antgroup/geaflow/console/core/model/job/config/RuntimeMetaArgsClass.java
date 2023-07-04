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

package com.antgroup.geaflow.console.core.model.job.config;

import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.ConfigValueBehavior;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.PluginConfigClass;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class RuntimeMetaArgsClass extends GeaflowConfigClass {

    private static final String RUNTIME_META_TABLE_NAME = "backend_meta";

    @GeaflowConfigKey(value = "geaflow.metric.stats.type", comment = "i18n.key.type")
    @GeaflowConfigValue(required = true, defaultValue = "JDBC")
    private GeaflowPluginType type;

    @GeaflowConfigKey(value = "geaflow.system.offset.backend.type", comment = "i18n.key.offset.storage.type")
    @GeaflowConfigValue(required = true, defaultValue = "JDBC")
    private GeaflowPluginType offsetMetaType;

    @GeaflowConfigKey(value = "geaflow.system.meta.table", comment = "i18n.key.table.name")
    @GeaflowConfigValue(required = true, defaultValue = RUNTIME_META_TABLE_NAME)
    private String table;

    @GeaflowConfigKey(value = "plugin", comment = "i18n.key.plugin.config")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private PluginConfigClass plugin;

    public RuntimeMetaArgsClass(GeaflowPluginConfig pluginConfig) {
        this.type = pluginConfig.getType();
        this.offsetMetaType = pluginConfig.getType();
        this.table = RUNTIME_META_TABLE_NAME;

        Class<? extends PluginConfigClass> configClass;
        switch (type) {
            case JDBC:
                configClass = JdbcPluginConfigClass.class;
                break;
            default:
                throw new GeaflowIllegalException("Runtime meta config type {} not supported", type);
        }

        this.plugin = pluginConfig.getConfig().parse(configClass);
    }
}
