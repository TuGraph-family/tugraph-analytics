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
import com.antgroup.geaflow.console.core.model.plugin.config.InfluxdbPluginConfigClass;
import com.antgroup.geaflow.console.core.model.plugin.config.PluginConfigClass;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class MetricArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.metric.reporters", comment = "i18n.key.metric.storage.type")
    @GeaflowConfigValue(required = true, defaultValue = "INFLUXDB")
    private GeaflowPluginType type;

    @GeaflowConfigKey(value = "plugin", comment = "i18n.key.plugin.config")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private PluginConfigClass plugin;

    public MetricArgsClass(GeaflowPluginConfig pluginConfig) {
        this.type = pluginConfig.getType();

        Class<? extends PluginConfigClass> configClass;
        switch (type) {
            case INFLUXDB:
                configClass = InfluxdbPluginConfigClass.class;
                break;
            default:
                throw new GeaflowIllegalException("Metric meta config type {} not supported", type);
        }

        this.plugin = pluginConfig.getConfig().parse(configClass);
    }
}
