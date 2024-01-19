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

import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ServeJobConfigClass extends JobConfigClass {

    @GeaflowConfigKey(value = "geaflow.job.mode", comment = "")
    @GeaflowConfigValue(required = true, defaultValue = "OLAP_SERVICE")
    private String jobMode;

    @GeaflowConfigKey(value = "geaflow.analytics.service.share.enable", comment = "")
    @GeaflowConfigValue(required = true, defaultValue = "true")
    private Boolean serviceShareEnable;

    @GeaflowConfigKey(value = "geaflow.analytics.graph.view.name", comment = "")
    @GeaflowConfigValue(required = true)
    private String graphName;

    @GeaflowConfigKey(value = "geaflow.analytics.query.parallelism", comment = "")
    private Integer queryParallelism;

    @GeaflowConfigKey(value = "geaflow.driver.num", comment = "")
    private Integer driverNum;

}
