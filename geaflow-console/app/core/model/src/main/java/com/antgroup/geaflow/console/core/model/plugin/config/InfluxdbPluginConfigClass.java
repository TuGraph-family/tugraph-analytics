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

import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InfluxdbPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.url", comment = "i18n.key.url")
    @GeaflowConfigValue(required = true, defaultValue = "http://0.0.0.0:8086")
    private String url;

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.token", comment = "i18n.key.token")
    @GeaflowConfigValue(required = true, masked = true)
    private String token;

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.org", comment = "i18n.key.organization")
    @GeaflowConfigValue(required = true, defaultValue = "geaflow")
    private String org;

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.bucket", comment = "i18n.key.bucket")
    @GeaflowConfigValue(required = true, defaultValue = "geaflow")
    private String bucket;

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.connect.timeout.ms", comment = "i18n.key.connect.timeout")
    @GeaflowConfigValue(defaultValue = "30000")
    private Integer connectTimeout;

    @GeaflowConfigKey(value = "geaflow.metric.influxdb.write.timeout.ms", comment = "i18n.key.write.timeout")
    @GeaflowConfigValue(defaultValue = "30000")
    private Integer writeTimeout;

    public InfluxdbPluginConfigClass() {
        super(GeaflowPluginType.INFLUXDB);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrl(url);
    }
}
