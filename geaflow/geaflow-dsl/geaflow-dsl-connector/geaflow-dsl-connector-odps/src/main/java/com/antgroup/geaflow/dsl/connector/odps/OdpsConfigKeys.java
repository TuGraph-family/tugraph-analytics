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

package com.antgroup.geaflow.dsl.connector.odps;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class OdpsConfigKeys {
    public static final ConfigKey GEAFLOW_DSL_ODPS_PROJECT = ConfigKeys
        .key("geaflow.dsl.odps.project")
        .noDefaultValue()
        .description("The odps project name.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_TABLE = ConfigKeys
        .key("geaflow.dsl.odps.table")
        .noDefaultValue()
        .description("The odps table name.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_ACCESS_ID = ConfigKeys
        .key("geaflow.dsl.odps.accessid")
        .noDefaultValue()
        .description("The odps accessid.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_ACCESS_KEY = ConfigKeys
        .key("geaflow.dsl.odps.accesskey")
        .noDefaultValue()
        .description("The odps accesskey.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_ENDPOINT = ConfigKeys
        .key("geaflow.dsl.odps.endpoint")
        .noDefaultValue()
        .description("The odps endpoint.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_PARTITION_SPEC = ConfigKeys
        .key("geaflow.dsl.odps.partition.spec")
        .defaultValue("")
        .description("The odps partition spec.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_SINK_BUFFER_SIZE = ConfigKeys
        .key("geaflow.dsl.odps.sink.buffer.size")
        .defaultValue(1000)
        .description("The buffer size of odps sink buffer.");

    public static final ConfigKey GEAFLOW_DSL_ODPS_TIMEOUT_SECONDS = ConfigKeys
        .key("geaflow.dsl.odps.timeout.seconds")
        .defaultValue(60)
        .description("The timeout for odps connection, in seconds.");
}
