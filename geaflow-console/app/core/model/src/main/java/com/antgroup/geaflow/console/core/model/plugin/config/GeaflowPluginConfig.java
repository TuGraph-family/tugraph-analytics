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

package com.antgroup.geaflow.console.core.model.plugin.config;

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.config.ConfigDescFactory;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowPluginConfig extends GeaflowName {

    private GeaflowPluginType type;

    private GeaflowPluginCategory category;

    private GeaflowConfig config;

    public GeaflowPluginConfig(GeaflowPluginCategory category, PluginConfigClass pluginConfigClass) {
        this.category = category;
        this.type = pluginConfigClass.getType();
        this.config = pluginConfigClass.build();
    }

    public GeaflowPluginConfig(String name, String comment, GeaflowPluginType type, GeaflowPluginCategory category,
                               GeaflowConfig config) {
        super.name = name;
        super.comment = comment;
        this.type = type;
        this.category = category;
        this.config = config;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(type, "Invalid plugin type");
        Preconditions.checkNotNull(category, "Invalid category");
        Preconditions.checkNotNull(config, "Invalid plugin config");
        ConfigDescFactory.get(type).validateConfig(config);
    }
}
