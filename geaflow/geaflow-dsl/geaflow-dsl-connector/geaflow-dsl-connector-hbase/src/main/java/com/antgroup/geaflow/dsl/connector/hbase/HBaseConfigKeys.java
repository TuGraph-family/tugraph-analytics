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

package com.antgroup.geaflow.dsl.connector.hbase;

import static com.antgroup.geaflow.dsl.connector.hbase.HBaseConstants.DEFAULT_BUFFER_SIZE;
import static com.antgroup.geaflow.dsl.connector.hbase.HBaseConstants.DEFAULT_FAMILY_MAPPING;
import static com.antgroup.geaflow.dsl.connector.hbase.HBaseConstants.DEFAULT_NAMESPACE;
import static com.antgroup.geaflow.dsl.connector.hbase.HBaseConstants.DEFAULT_SEPARATOR;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class HBaseConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_HBASE_ZOOKEEPER_QUORUM = ConfigKeys
        .key("geaflow.dsl.hbase.zookeeper.quorum")
        .noDefaultValue()
        .description("HBase zookeeper quorum servers list.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_NAME_SPACE = ConfigKeys
        .key("geaflow.dsl.hbase.namespace")
        .defaultValue(DEFAULT_NAMESPACE)
        .description("HBase namespace.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.hbase.tablename")
        .noDefaultValue()
        .description("HBase table name.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_ROWKEY_COLUMNS = ConfigKeys
        .key("geaflow.dsl.hbase.rowkey.column")
        .noDefaultValue()
        .description("HBase rowkey columns.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_ROWKEY_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.hbase.rowkey.separator")
        .defaultValue(DEFAULT_SEPARATOR)
        .description("HBase rowkey join serapator.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_FAMILY_NAME = ConfigKeys
        .key("geaflow.dsl.hbase.familyname.mapping")
        .defaultValue(DEFAULT_FAMILY_MAPPING)
        .description("HBase column family name mapping.");

    public static final ConfigKey GEAFLOW_DSL_HBASE_BUFFER_SIZE = ConfigKeys
        .key("geaflow.dsl.hbase.buffersize")
        .defaultValue(DEFAULT_BUFFER_SIZE)
        .description("HBase writer buffer size.");
}
