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

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.ConfigValueBehavior;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class JobArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.system.state.backend.type", comment = "i18n.key.state.storage.type")
    @GeaflowConfigValue(required = true, defaultValue = "ROCKSDB")
    private GeaflowPluginType systemStateType;

    @GeaflowConfigKey(value = "jobConfig", comment = "i18n.key.task.user.params")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private JobConfigClass jobConfig;

    public JobArgsClass(JobConfigClass jobConfig) {
        this.jobConfig = jobConfig;
    }
}
