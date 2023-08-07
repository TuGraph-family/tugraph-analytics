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
import com.antgroup.geaflow.console.core.model.plugin.config.DfsPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.LocalPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.OssPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.PersistentPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.PluginConfigClass;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class PersistentArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.file.persistent.type", comment = "i18n.key.storage.type")
    @GeaflowConfigValue(required = true, defaultValue = "LOCAL")
    private GeaflowPluginType type;

    @GeaflowConfigKey(value = "geaflow.file.persistent.root", comment = "i18n.key.root.path")
    @GeaflowConfigValue(required = true, defaultValue = "/geaflow/chk")
    private String root;

    @GeaflowConfigKey(value = "geaflow.file.persistent.thread.size", comment = "i18n.key.local.thread.pool.count")
    @GeaflowConfigValue
    private Integer threadSize;

    @GeaflowConfigKey(value = "geaflow.file.persistent.user.name", comment = "i18n.key.username")
    @GeaflowConfigValue(defaultValue = "geaflow")
    private String username;

    @GeaflowConfigKey(value = "geaflow.file.persistent.config.json", comment = "i18n.key.ext.config.json")
    @GeaflowConfigValue(behavior = ConfigValueBehavior.JSON)
    private PluginConfigClass plugin;

    public PersistentArgsClass(GeaflowPluginConfig pluginConfig) {
        this.type = pluginConfig.getType();

        Class<? extends PersistentPluginConfigClass> configClass;
        switch (type) {
            case LOCAL:
                configClass = LocalPluginConfigClass.class;
                break;
            case DFS:
                configClass = DfsPluginConfigClass.class;
                break;
            case OSS:
                configClass = OssPluginConfigClass.class;
                break;
            default:
                throw new GeaflowIllegalException("Persistent config type {} not supported", type);
        }

        PersistentPluginConfigClass config = pluginConfig.getConfig().parse(configClass);
        this.root = config.getRoot();
        this.threadSize = config.getThreadSize();
        this.username = config.getUsername();
        this.plugin = config;
    }
}
