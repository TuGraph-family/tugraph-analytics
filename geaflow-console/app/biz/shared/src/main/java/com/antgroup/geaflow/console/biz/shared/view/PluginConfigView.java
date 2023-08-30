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

package com.antgroup.geaflow.console.biz.shared.view;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import java.util.Locale;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PluginConfigView extends NameView {

    private String type;

    @Getter
    @Setter
    private GeaflowConfig config;

    @Getter
    @Setter
    private GeaflowPluginCategory category;

    public GeaflowPluginType getType() {
        GeaflowPluginType pluginType;
        try {
            pluginType = GeaflowPluginType.valueOf(type.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new GeaflowException("Cannot find relate plugin type: {}", type, e);
        }
        return pluginType;
    }

    public void setType(GeaflowPluginType type) {
        this.type = type.name().toUpperCase(Locale.ROOT);
    }
}
