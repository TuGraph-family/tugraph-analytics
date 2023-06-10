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

package com.antgroup.geaflow.dsl.runtime.testenv;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.api.function.GeaFlowTableSinkFunction;
import com.antgroup.geaflow.dsl.runtime.query.KafkaFoTest;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoGeaFlowTableSinkFunction extends GeaFlowTableSinkFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        FoGeaFlowTableSinkFunction.class);

    public FoGeaFlowTableSinkFunction(GeaFlowTable table, TableSink tableSink) {
        super(table, tableSink);
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        LOGGER.info("open fo sink function.");
        super.open(runtimeContext);
    }

    @Override
    public void write(Row row) throws Exception {
        super.write(row);
        if (KafkaFoTest.injectExceptionTimes > 0) {
            --KafkaFoTest.injectExceptionTimes;
            throw new RuntimeException("break for fo test");
        }
    }
}
