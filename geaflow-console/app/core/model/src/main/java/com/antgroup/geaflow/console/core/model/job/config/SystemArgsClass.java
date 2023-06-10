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
public class SystemArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.job.unique.id", comment = "作业ID")
    @GeaflowConfigValue(required = true)
    private String taskId;

    @GeaflowConfigKey(value = "geaflow.job.cluster.id", comment = "运行时作业ID")
    @GeaflowConfigValue(required = true)
    private String runtimeTaskId;

    @GeaflowConfigKey(value = "geaflow.job.runtime.name", comment = "运行时作业名")
    @GeaflowConfigValue(required = true)
    private String runtimeTaskName;

    @GeaflowConfigKey(value = "geaflow.gw.endpoint", comment = "API服务地址")
    @GeaflowConfigValue(required = true, defaultValue = "http://0.0.0.0:8080")
    private String gateway;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.token.key", comment = "API服务Token")
    @GeaflowConfigValue(required = true, masked = true)
    private String taskToken;

    @GeaflowConfigKey(value = "geaflow.cluster.started.callback.url", comment = "启动通知服务URL")
    @GeaflowConfigValue(required = true)
    private String startupNotifyUrl;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.instance.name", comment = "默认实例名")
    @GeaflowConfigValue(required = true)
    private String instanceName;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.type", comment = "作业Catalog存储类型")
    @GeaflowConfigValue(required = true, defaultValue = "console")
    private String catalogType;

    @GeaflowConfigKey(value = "stateConfig", comment = "State参数")
    @GeaflowConfigValue(required = true)
    private StateArgsClass stateArgs;

    @GeaflowConfigKey(value = "metricConfig", comment = "Metric参数")
    @GeaflowConfigValue(required = true)
    private MetricArgsClass metricArgs;

}
