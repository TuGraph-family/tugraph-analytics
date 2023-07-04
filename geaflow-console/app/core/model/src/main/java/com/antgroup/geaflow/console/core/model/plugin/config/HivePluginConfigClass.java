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
public class HivePluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.hive.metastore.uris", comment = "i18n.key.metastore.address")
    @GeaflowConfigValue(required = true, defaultValue = "thrift://localhost:9083")
    private String metastore;

    @GeaflowConfigKey(value = "geaflow.dsl.hive.database.name", comment = "i18n.key.database.name")
    @GeaflowConfigValue(required = true)
    private String database;

    @GeaflowConfigKey(value = "geaflow.dsl.hive.table.name", comment = "i18n.key.table.name")
    @GeaflowConfigValue(required = true)
    private String table;

    @GeaflowConfigKey(value = "geaflow.dsl.hive.splits.per.partition", comment = "i18n.key.read.splits.per.partition")
    @GeaflowConfigValue(defaultValue = "1")
    private Integer partition;

    public HivePluginConfigClass() {
        super(GeaflowPluginType.HIVE);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrls(metastore, ",");
    }
}
