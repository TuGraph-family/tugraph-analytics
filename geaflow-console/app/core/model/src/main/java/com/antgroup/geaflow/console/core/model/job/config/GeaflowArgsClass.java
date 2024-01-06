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
public class GeaflowArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "system", comment = "i18n.key.system.params")
    @GeaflowConfigValue(required = true)
    private SystemArgsClass systemArgs;

    @GeaflowConfigKey(value = "cluster", comment = "i18n.key.cluster.args")
    @GeaflowConfigValue(required = true)
    private ClusterArgsClass clusterArgs;

    @GeaflowConfigKey(value = "job", comment = "i18n.key.task.params")
    @GeaflowConfigValue(required = true)
    private JobArgsClass jobArgs;

}
