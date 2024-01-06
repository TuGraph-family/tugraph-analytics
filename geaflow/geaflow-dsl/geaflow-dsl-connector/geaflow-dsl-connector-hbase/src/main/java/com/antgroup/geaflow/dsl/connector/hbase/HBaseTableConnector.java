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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.api.TableWritableConnector;

public class HBaseTableConnector implements TableWritableConnector {

    public static final String TYPE = "HBase";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public TableSink createSink(Configuration conf) {
        return new HBaseTableSink();
    }
}
