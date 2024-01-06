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

public class ConnectorConfigKeys implements Serializable {

    /*************************************************
     *  Connectors Common Parameters.
     *************************************************/
    public static final ConfigKey GEAFLOW_DSL_LINE_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.line.separator")
        .defaultValue("\n")
        .description("The line separator for split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_COLUMN_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.column.separator")
        .defaultValue(",")
        .description("The column separator for split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_PARTITIONS_PER_SOURCE_PARALLELISM = ConfigKeys
        .key("geaflow.dsl.partitions.per.source.parallelism")
        .defaultValue(1)
        .description("Partitions to read for each source parallelism.");

    public static final ConfigKey GEAFLOW_DSL_COLUMN_TRIM = ConfigKeys
        .key("geaflow.dsl.column.trim")
        .defaultValue(true)
        .description("Whether doing trim operation for column text when split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_START_TIME = ConfigKeys
        .key("geaflow.dsl.start.time")
        .defaultValue("begin")
        .description("Specifies the starting unix timestamp for reading the data table. Format "
            + "must be 'yyyy-MM-dd HH:mm:ss'.");

    /*************************************************
     *  FILE Connector Parameters.
     *************************************************/
    public static final ConfigKey GEAFLOW_DSL_FILE_PATH = ConfigKeys
        .key("geaflow.dsl.file.path")
        .noDefaultValue()
        .description("The file path for the file table.");

    public static final ConfigKey GEAFLOW_DSL_FILE_NAME_REGEX = ConfigKeys
        .key("geaflow.dsl.file.name.regex")
        .defaultValue("")
        .description("The regular expression for filtering the files in the path.");

    public static final ConfigKey GEAFLOW_DSL_FILE_FORMAT = ConfigKeys
        .key("geaflow.dsl.file.format")
        .defaultValue("txt")
        .description("The file format to read or write, default value is 'txt'. ");

    public static final ConfigKey GEAFLOW_DSL_SKIP_HEADER = ConfigKeys
        .key("geaflow.dsl.skip.header")
        .defaultValue(false)
        .description("Whether skip the header for csv format.");
}
