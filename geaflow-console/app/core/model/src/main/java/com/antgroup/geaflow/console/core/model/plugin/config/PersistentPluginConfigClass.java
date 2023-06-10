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

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class PersistentPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.file.persistent.root", comment = "Root路径", jsonIgnore = true)
    @GeaflowConfigValue(required = true, defaultValue = "/")
    private String root;

    @GeaflowConfigKey(value = "geaflow.file.persistent.user.name", comment = "用户名", jsonIgnore = true)
    @GeaflowConfigValue
    private String username;

    @GeaflowConfigKey(value = "geaflow.file.persistent.thread.size", comment = "本地线程池大小", jsonIgnore = true)
    @GeaflowConfigValue
    private Integer threadSize;

    public PersistentPluginConfigClass(GeaflowPluginType type) {
        super(type);
    }
}
