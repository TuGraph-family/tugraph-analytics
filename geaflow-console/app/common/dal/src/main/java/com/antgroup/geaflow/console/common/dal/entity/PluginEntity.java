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

package com.antgroup.geaflow.console.common.dal.entity;

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("geaflow_plugin")
public class PluginEntity extends UserLevelEntity {

    private GeaflowPluginType pluginType;

    private GeaflowPluginCategory pluginCategory;

    private String jarPackageId;

    private String version;

    private String dataPluginId;

    private String entryClass;
}
