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

import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CompileContextClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.token.key", comment = "i18n.key.api.token")
    @GeaflowConfigValue(required = true)
    private String tokenKey;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.instance.name", comment = "i18n.key.default.instance.name")
    @GeaflowConfigValue(required = true)
    private String instanceName;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.type", comment = "i18n.key.job.catalog.type")
    @GeaflowConfigValue(required = true, defaultValue = "memory")
    private String catalogType;

    @GeaflowConfigKey(value = "geaflow.gw.endpoint", comment = "i18n.key.k8s.server.url")
    @GeaflowConfigValue(required = true)
    private String endpoint;


}
