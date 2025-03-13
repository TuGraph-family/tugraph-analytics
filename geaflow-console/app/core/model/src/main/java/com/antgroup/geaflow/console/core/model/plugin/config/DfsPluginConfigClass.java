/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.console.core.model.plugin.config;

import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class DfsPluginConfigClass extends PersistentPluginConfigClass {

    public static final String DFS_URI_KEY = "fs.defaultFS";

    @GeaflowConfigKey(value = DFS_URI_KEY, comment = "i18n.key.dfs.address")
    @GeaflowConfigValue(required = true, defaultValue = "hdfs://0.0.0.0:9000")
    private String defaultFs;

    public DfsPluginConfigClass() {
        super(GeaflowPluginType.DFS);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrl(defaultFs);
    }
}
