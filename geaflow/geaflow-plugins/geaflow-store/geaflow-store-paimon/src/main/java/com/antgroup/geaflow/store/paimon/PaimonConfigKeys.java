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

package com.antgroup.geaflow.store.paimon;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class PaimonConfigKeys {

    public static final ConfigKey PAIMON_OPTIONS_WAREHOUSE = ConfigKeys
        .key("geaflow.store.paimon.options.warehouse")
        .defaultValue("file:///tmp/paimon/")
        .description("paimon warehouse, default LOCAL path, now support path prefix: "
            + "[file://], Options for future: [hdfs://, oss://, s3://]");

    public static final ConfigKey PAIMON_OPTIONS_META_STORE = ConfigKeys
        .key("geaflow.store.paimon.options.meta.store")
        .defaultValue("FILESYSTEM")
        .description("Metastore of paimon catalog, now support [FILESYSTEM]. Options for future: "
            + "[HIVE, JDBC].");

    public static final ConfigKey PAIMON_OPTIONS_MEMTABLE_SIZE_MB = ConfigKeys
        .key("geaflow.store.paimon.memtable.size.mb")
        .defaultValue(128)
        .description("paimon memtable size, default 256MB");
}
