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
public class OssPluginConfigClass extends PersistentPluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.file.oss.endpoint", comment = "i18n.key.endpoint")
    @GeaflowConfigValue(required = true)
    private String endpoint;

    @GeaflowConfigKey(value = "geaflow.file.oss.access.id", comment = "i18n.key.access.id")
    @GeaflowConfigValue(required = true)
    private String accessId;

    @GeaflowConfigKey(value = "geaflow.file.oss.secret.key", comment = "i18n.key.secret.key")
    @GeaflowConfigValue(required = true, masked = true)
    private String secretKey;

    @GeaflowConfigKey(value = "geaflow.file.oss.bucket.name", comment = "i18n.key.bucket")
    @GeaflowConfigValue(required = true)
    private String bucket;

    public OssPluginConfigClass() {
        super(GeaflowPluginType.OSS);
    }

    @Override
    public void testConnection() {
        if (NetworkUtil.getPort(endpoint) == null) {
            String host = NetworkUtil.getHost(endpoint);
            NetworkUtil.testHostPort(host, 80);

        } else {
            NetworkUtil.testUrl(endpoint);
        }
    }
}
