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

package com.antgroup.geaflow.console.core.model.install;

import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowInstall extends GeaflowId {

    private GeaflowPluginConfig runtimeClusterConfig;

    private GeaflowPluginConfig runtimeMetaConfig;

    private GeaflowPluginConfig haMetaConfig;

    private GeaflowPluginConfig metricConfig;

    private GeaflowPluginConfig remoteFileConfig;

    private GeaflowPluginConfig dataConfig;

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(runtimeClusterConfig, "Invalid runtimeClusterConfig");
        Preconditions.checkNotNull(runtimeMetaConfig, "Invalid runtimeMetaConfig");
        Preconditions.checkNotNull(haMetaConfig, "Invalid haMetaConfig");
        Preconditions.checkNotNull(metricConfig, "Invalid metricConfig");
        Preconditions.checkNotNull(remoteFileConfig, "Invalid remoteFileConfig");
        Preconditions.checkNotNull(dataConfig, "Invalid dataConfig");
    }
}
