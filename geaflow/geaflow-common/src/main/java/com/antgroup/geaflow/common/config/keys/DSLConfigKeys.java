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

package com.antgroup.geaflow.common.config.keys;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;
import java.io.Serializable;

public class DSLConfigKeys implements Serializable {

    public static final ConfigKey GEAFLOW_DSL_STORE_TYPE = ConfigKeys
        .key("geaflow.dsl.graph.store.type")
        .noDefaultValue()
        .description("The graph store type.");

    public static final ConfigKey GEAFLOW_DSL_STORE_SHARD_COUNT = ConfigKeys
        .key("geaflow.dsl.graph.store.shard.count")
        .defaultValue(2)
        .description("The graph store shard count.");

    public static final ConfigKey GEAFLOW_DSL_WINDOW_SIZE = ConfigKeys
        .key("geaflow.dsl.window.size")
        .defaultValue(1L)
        .description("Window size, -1 represent the all window.");

    public static final ConfigKey GEAFLOW_DSL_TABLE_TYPE = ConfigKeys
        .key("geaflow.dsl.table.type")
        .noDefaultValue()
        .description("The table type.");

    public static final ConfigKey GEAFLOW_DSL_MAX_TRAVERSAL = ConfigKeys
        .key("geaflow.dsl.max.traversal")
        .defaultValue(64)
        .description("The max traversal count.");

    public static final ConfigKey GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION = ConfigKeys
        .key("geaflow.dsl.custom.source.function")
        .noDefaultValue()
        .description("Custom source function class name.");

    public static final ConfigKey GEAFLOW_DSL_CUSTOM_SINK_FUNCTION = ConfigKeys
        .key("geaflow.dsl.custom.sink.function")
        .noDefaultValue()
        .description("Custom sink function class name.");

    public static final ConfigKey GEAFLOW_DSL_QUERY_PATH = ConfigKeys
        .key("geaflow.dsl.query.path")
        .noDefaultValue()
        .description("The gql query path.");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_TYPE = ConfigKeys
        .key("geaflow.dsl.catalog.type")
        .defaultValue("memory")
        .description("The catalog type name. Optional internal implementations "
            + "include 'memory' and 'console'");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_TOKEN_KEY = ConfigKeys
        .key("geaflow.dsl.catalog.token.key")
        .noDefaultValue()
        .description("The catalog token key set by console platform");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_INSTANCE_NAME = ConfigKeys
        .key("geaflow.dsl.catalog.instance.name")
        .defaultValue("default")
        .description("The default instance name set by console platform");

    public static final ConfigKey GEAFLOW_DSL_SKIP_EXCEPTION = ConfigKeys
        .key("geaflow.dsl.ignore.exception")
        .defaultValue(false)
        .description("If set true, dsl will skip the exception for dirty data.");

    public static final ConfigKey GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE = ConfigKeys
        .key("geaflow.dsl.traversal.all.split.enable")
        .defaultValue(false)
        .description("Whether enable the split of the ids for traversal all. ");
}
