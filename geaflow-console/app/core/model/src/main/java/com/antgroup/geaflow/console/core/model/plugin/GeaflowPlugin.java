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

package com.antgroup.geaflow.console.core.model.plugin;

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowPlugin extends GeaflowName {

    private GeaflowPluginType type;

    private GeaflowPluginCategory category;

    private String version;

    private GeaflowRemoteFile jarPackage;

    private GeaflowPlugin dataPlugin;

    private String entryClass;
}
