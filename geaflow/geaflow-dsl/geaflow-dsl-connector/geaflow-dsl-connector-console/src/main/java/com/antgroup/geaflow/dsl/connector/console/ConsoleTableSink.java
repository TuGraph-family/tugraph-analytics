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

package com.antgroup.geaflow.dsl.connector.console;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleTableSink.class);

    private boolean skip;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        skip = tableConf.getBoolean(ConsoleConfigKeys.GEAFLOW_DSL_CONSOLE_SKIP);
    }

    @Override
    public void open(RuntimeContext context) {

    }

    @Override
    public void write(Row row) {
        if (!skip) {
            LOGGER.info(row.toString());
        }
    }

    @Override
    public void finish() {

    }

    @Override
    public void close() {

    }
}
