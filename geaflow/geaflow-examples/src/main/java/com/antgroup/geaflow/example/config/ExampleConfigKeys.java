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

package com.antgroup.geaflow.example.config;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

import java.io.Serializable;

public class ExampleConfigKeys implements Serializable {

    public static final ConfigKey SOURCE_PARALLELISM = ConfigKeys
        .key("geaflow.source.parallelism")
        .defaultValue(1)
        .description("job source parallelism");

    public static final ConfigKey SINK_PARALLELISM = ConfigKeys
        .key("geaflow.sink.parallelism")
        .defaultValue(1)
        .description("job sink parallelism");

    public static final ConfigKey MAP_PARALLELISM = ConfigKeys
        .key("geaflow.map.parallelism")
        .defaultValue(1)
        .description("job map parallelism");

    public static final ConfigKey REDUCE_PARALLELISM = ConfigKeys
        .key("geaflow.reduce.parallelism")
        .defaultValue(1)
        .description("job reduce parallelism");

    public static final ConfigKey ITERATOR_PARALLELISM = ConfigKeys
        .key("geaflow.iterator.parallelism")
        .defaultValue(1)
        .description("job iterator parallelism");

    public static final ConfigKey AGG_PARALLELISM = ConfigKeys
        .key("geaflow.agg.parallelism")
        .defaultValue(1)
        .description("job agg parallelism");

    public static final ConfigKey GEAFLOW_SINK_TYPE = ConfigKeys
        .key("geaflow.sink.type")
        .defaultValue("console")
        .description("job sink type, console or file");

}

