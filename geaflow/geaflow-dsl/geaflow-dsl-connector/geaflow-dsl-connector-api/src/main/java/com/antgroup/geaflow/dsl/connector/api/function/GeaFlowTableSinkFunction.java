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

package com.antgroup.geaflow.dsl.connector.api.function;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichWindowFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.metrics.common.MetricConstants;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Histogram;
import com.antgroup.geaflow.metrics.common.api.Meter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of {@link SinkFunction} for DSL table sink.
 */
public class GeaFlowTableSinkFunction extends RichWindowFunction implements SinkFunction<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowTableSinkFunction.class);

    protected final GeaFlowTable table;

    protected TableSink tableSink;

    private boolean skipWrite;
    private Histogram writeRt;
    private Histogram flushRt;
    private Meter writeTps;

    public GeaFlowTableSinkFunction(GeaFlowTable table, TableSink tableSink) {
        this.table = table;
        this.tableSink = tableSink;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        tableSink.open(runtimeContext);
        LOGGER.info("open sink table: {}", table.getName());
        writeRt = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .histogram(MetricNameFormatter.tableWriteTimeRtName(table.getName()));
        flushRt = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .histogram(MetricNameFormatter.tableFlushTimeRtName(table.getName()));
        writeTps = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .meter(MetricNameFormatter.tableOutputRowTpsName(table.getName()));
        Configuration conf = table.getConfigWithGlobal(runtimeContext.getConfiguration());
        skipWrite = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SINK_ENABLE_SKIP);
    }

    @Override
    public void write(Row row) throws Exception {
        if (!skipWrite) {
            long startTime = System.currentTimeMillis();
            tableSink.write(row);
            writeRt.update(System.currentTimeMillis() - startTime);
            writeTps.mark();
        }
    }

    @Override
    public void finish() {
        try {
            long startTime = System.currentTimeMillis();
            tableSink.finish();
            flushRt.update(System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in sink flush", e);
        }
    }

    @Override
    public void close() {
        tableSink.close();
    }
}
